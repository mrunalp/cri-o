#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <syslog.h>
#include <unistd.h>

#include <glib.h>
#include <json-glib/json-glib.h>

#include "cmsg.h"

#define pexit(fmt, ...)                                                          \
	do {                                                                     \
		fprintf(stderr, "[conmon:e]: " fmt " %m\n", ##__VA_ARGS__);      \
		syslog(LOG_ERR, "conmon <error>: " fmt ": %m\n", ##__VA_ARGS__); \
		exit(EXIT_FAILURE);                                              \
	} while (0)

#define nexit(fmt, ...)                                                       \
	do {                                                                  \
		fprintf(stderr, "[conmon:e]: " fmt "\n", ##__VA_ARGS__);      \
		syslog(LOG_ERR, "conmon <error>: " fmt " \n", ##__VA_ARGS__); \
		exit(EXIT_FAILURE);                                           \
	} while (0)

#define nwarn(fmt, ...)                                                        \
	do {                                                                   \
		fprintf(stderr, "[conmon:w]: " fmt "\n", ##__VA_ARGS__);       \
	} while (0)

#define ninfo(fmt, ...)                                                        \
	do {                                                                   \
		fprintf(stderr, "[conmon:i]: " fmt "\n", ##__VA_ARGS__);       \
	} while (0)

#define _cleanup_(x) __attribute__((cleanup(x)))

static inline void freep(void *p)
{
	free(*(void **)p);
}

static inline void closep(int *fd)
{
	if (*fd >= 0)
		close(*fd);
	*fd = -1;
}

static inline void fclosep(FILE **fp) {
	if (*fp)
		fclose(*fp);
	*fp = NULL;
}

static inline void gstring_free_cleanup(GString **string)
{
	if (*string)
		g_string_free(*string, TRUE);
}

#define _cleanup_free_ _cleanup_(freep)
#define _cleanup_close_ _cleanup_(closep)
#define _cleanup_fclose_ _cleanup_(fclosep)
#define _cleanup_gstring_ _cleanup_(gstring_free_cleanup)

#define BUF_SIZE 256
#define CMD_SIZE 1024
#define MAX_EVENTS 10

static bool terminal = false;
static char *cid = NULL;
static char *runtime_path = NULL;
static char *bundle_path = NULL;
static char *pid_file = NULL;
static bool systemd_cgroup = false;
static bool exec = false;
static char *log_path = NULL;
static GOptionEntry entries[] =
{
  { "terminal", 't', 0, G_OPTION_ARG_NONE, &terminal, "Terminal", NULL },
  { "cid", 'c', 0, G_OPTION_ARG_STRING, &cid, "Container ID", NULL },
  { "runtime", 'r', 0, G_OPTION_ARG_STRING, &runtime_path, "Runtime path", NULL },
  { "bundle", 'b', 0, G_OPTION_ARG_STRING, &bundle_path, "Bundle path", NULL },
  { "pidfile", 'p', 0, G_OPTION_ARG_STRING, &pid_file, "PID file", NULL },
  { "systemd-cgroup", 's', 0, G_OPTION_ARG_NONE, &systemd_cgroup, "Enable systemd cgroup manager", NULL },
  { "exec", 'e', 0, G_OPTION_ARG_NONE, &exec, "Exec a command in a running container", NULL },
  { "log-path", 'l', 0, G_OPTION_ARG_STRING, &log_path, "Log file path", NULL },
  { NULL }
};

/* strlen("1997-03-25T13:20:42.999999999+01:00") + 1 */
#define TSBUFLEN 36

#define CGROUP_ROOT "/sys/fs/cgroup"

int set_k8s_timestamp(char *buf, ssize_t buflen)
{
	struct tm *tm;
	struct timespec ts;
	char off_sign = '+';
	int off, len, err = -1;

	if (clock_gettime(CLOCK_REALTIME, &ts) < 0) {
		/* If CLOCK_REALTIME is not supported, we set nano seconds to 0 */
		if (errno == EINVAL) {
			ts.tv_nsec = 0;
		} else {
			return err;
		}
	}

	if ((tm = localtime(&ts.tv_sec)) == NULL)
		return err;


	off = (int) tm->tm_gmtoff;
	if (tm->tm_gmtoff < 0) {
		off_sign = '-';
		off = -off;
	}

	len = snprintf(buf, buflen, "%d-%02d-%02dT%02d:%02d:%02d.%09ld%c%02d:%02d",
		       tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
		       tm->tm_hour, tm->tm_min, tm->tm_sec, ts.tv_nsec,
		       off_sign, off / 3600, off % 3600);

	if (len < buflen)
		err = 0;
	return err;
}

/* stdpipe_t represents one of the std pipes (or NONE). */
typedef enum {
	NO_PIPE,
	STDIN_PIPE, /* unused */
	STDOUT_PIPE,
	STDERR_PIPE,
} stdpipe_t;

const char *stdpipe_name(stdpipe_t pipe)
{
	switch (pipe) {
	case STDIN_PIPE:
		return "stdin";
	case STDOUT_PIPE:
		return "stdout";
	case STDERR_PIPE:
		return "stderr";
	default:
		return "NONE";
	}
}

/*
 * The CRI requires us to write logs with a (timestamp, stream, line) format
 * for every newline-separated line. write_k8s_log writes said format for every
 * line in buf, and will partially write the final line of the log if buf is
 * not terminated by a newline.
 */
int write_k8s_log(int fd, stdpipe_t pipe, const char *buf, ssize_t buflen)
{
	char tsbuf[TSBUFLEN];
	static stdpipe_t trailing_line = NO_PIPE;

	/*
	 * Use the same timestamp for every line of the log in this buffer.
	 * There is no practical difference in the output since write(2) is
	 * fast.
	 */
	if (set_k8s_timestamp(tsbuf, TSBUFLEN))
		/* TODO: We should handle failures much more cleanly than this. */
		return -1;

	while (buflen > 0) {
		const char *line_end = NULL;
		ptrdiff_t line_len = 0;

		/* Find the end of the line, or alternatively the end of the buffer. */
		line_end = memchr(buf, '\n', buflen);
		if (line_end == NULL)
			line_end = &buf[buflen-1];
		line_len = line_end - buf + 1;

		/*
		 * Write the (timestamp, stream) tuple if there isn't any trailing
		 * output from the previous line (or if there is trailing output but
		 * the current buffer being printed is from a different pipe).
		 */
		if (trailing_line != pipe) {
			/*
			 * If there was a trailing line from a different pipe, prepend a
			 * newline to split it properly. This technically breaks the flow
			 * of the previous line (adding a newline in the log where there
			 * wasn't one output) but without modifying the file in a
			 * non-append-only way there's not much we can do.
			 */
			char *leading = "";
			if (trailing_line != NO_PIPE)
				leading = "\n";

			if (dprintf(fd, "%s%s %s ", leading, tsbuf, stdpipe_name(pipe)) < 0) {
				nwarn("failed to write (timestamp, stream) to log");
				goto next;
			}
		}

		/* Output the actual contents. */
		if (write(fd, buf, line_len) < 0) {
			nwarn("failed to write buffer to log");
			goto next;
		}

		/* If we did not output a full line, then we are a trailing_line. */
		trailing_line = (*line_end == '\n') ? NO_PIPE : pipe;

next:
		/* Update the head of the buffer remaining to output. */
		buf += line_len;
		buflen -= line_len;
	}

	return 0;
}

/*
 * Returns the path for specified controller name for a pid.
 * Returns NULL on error.
 */
static char *process_cgroup_subsystem_path(int pid, const char *subsystem) {
	_cleanup_free_ char *cgroups_file_path = NULL;
	int rc;
	rc = asprintf(&cgroups_file_path, "/proc/%d/cgroup", pid);
	if (rc < 0) {
		nwarn("Failed to allocate memory for cgroups file path");
		return NULL;
	}

	_cleanup_fclose_ FILE *fp = NULL;
	fp = fopen(cgroups_file_path, "r");
	if (fp == NULL) {
		nwarn("Failed to open cgroups file: %s", cgroups_file_path);
		return NULL;
	}

	_cleanup_free_ char *line = NULL;
	ssize_t read;
	size_t len = 0;
	char *ptr;
	char *subsystem_path = NULL;
	while ((read = getline(&line, &len, fp)) != -1) {
		ptr = strchr(line, ':');
		if (ptr == NULL) {
			nwarn("Error parsing cgroup, ':' not found: %s", line);
			return NULL;
		}
		ptr++;
		if (!strncmp(ptr, subsystem, strlen(subsystem))) {
			char *path = strchr(ptr, '/');
			if (path == NULL) {
				nwarn("Error finding path in cgroup: %s", line);
				return NULL;
			}
			ninfo("PATH: %s", path);
			const char *subpath = strchr(subsystem, '=');
			if (subpath == NULL) {
				subpath = subsystem;
			} else {
				subpath++;
			}

			rc = asprintf(&subsystem_path, "%s/%s%s", CGROUP_ROOT, subpath, path);
			if (rc < 0) {
				nwarn("Failed to allocate memory for subsystemd path");
				return NULL;
			}
			ninfo("SUBSYSTEM_PATH: %s", subsystem_path);
			subsystem_path[strlen(subsystem_path) - 1] = '\0';
			return subsystem_path;
		}
	}

	return NULL;
}

int main(int argc, char *argv[])
{
	int ret, runtime_status;
	char cwd[PATH_MAX];
	char default_pid_file[PATH_MAX];
	char attach_sock_path[PATH_MAX];
	char ctl_fifo_path[PATH_MAX];
	GError *err = NULL;
	_cleanup_free_ char *contents;
	int cpid = -1;
	int status;
	pid_t pid, create_pid;
	_cleanup_close_ int logfd = -1;
	_cleanup_close_ int masterfd_stdout = -1;
	_cleanup_close_ int masterfd_stderr = -1;
	_cleanup_close_ int masterfd_stdin = -1;
	_cleanup_close_ int epfd = -1;
	_cleanup_close_ int csfd = -1;
	/* Used for !terminal cases. */
	int slavefd_stdout = -1;
	int slavefd_stderr = -1;
	int slavefd_stdin = -1;
	char csname[PATH_MAX] = "/tmp/conmon-term.XXXXXXXX";
	char buf[BUF_SIZE];
	int num_read;
	struct epoll_event ev;
	struct epoll_event evlist[MAX_EVENTS];
	int sync_pipe_fd = -1;
	char *sync_pipe, *endptr;
	int len;
	int num_stdio_fds = 0;
	GError *error = NULL;
	GOptionContext *context;
	_cleanup_gstring_ GString *cmd = NULL;

	/* Used for OOM notification API */
	_cleanup_close_ int efd = -1;
	_cleanup_close_ int cfd = -1;
	_cleanup_close_ int ofd = -1;
	_cleanup_free_ char *memory_cgroup_path = NULL;
	int wb;
	uint64_t oom_event;

	/* Used for attach */
	_cleanup_close_ int conn_sock = -1;

	/* Command line parameters */
	context = g_option_context_new("- conmon utility");
	g_option_context_add_main_entries(context, entries, "conmon");
	if (!g_option_context_parse(context, &argc, &argv, &error)) {
	        g_print("option parsing failed: %s\n", error->message);
	        exit(1);
	}

	if (cid == NULL)
		nexit("Container ID not provided. Use --cid");

	if (runtime_path == NULL)
		nexit("Runtime path not provided. Use --runtime");

	if (bundle_path == NULL && !exec) {
		if (getcwd(cwd, sizeof(cwd)) == NULL) {
			nexit("Failed to get working directory");
		}
		bundle_path = cwd;
	}

	if (pid_file == NULL) {
		if (snprintf(default_pid_file, sizeof(default_pid_file),
			     "%s/pidfile-%s", cwd, cid) < 0) {
			nexit("Failed to generate the pidfile path");
		}
		pid_file = default_pid_file;
	}

	if (log_path == NULL)
		nexit("Log file path not provided. Use --log-path");

	/* Environment variables */
	sync_pipe = getenv("_OCI_SYNCPIPE");
	if (sync_pipe) {
		errno = 0;
		sync_pipe_fd = strtol(sync_pipe, &endptr, 10);
		if (errno != 0 || *endptr != '\0')
			pexit("unable to parse _OCI_SYNCPIPE");
	}

	/* Open the log path file. */
	logfd = open(log_path, O_WRONLY | O_APPEND | O_CREAT);
	if (logfd < 0)
		pexit("Failed to open log file");

	/*
	 * Set self as subreaper so we can wait for container process
	 * and return its exit code.
	 */
	ret = prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0);
	if (ret != 0) {
		pexit("Failed to set as subreaper");
	}

	if (terminal) {
		struct sockaddr_un addr = {0};

		/*
		 * Generate a temporary name. Is this unsafe? Probably, but we can
		 * replace it with a rename(2) setup if necessary.
		 */

		int unusedfd = g_mkstemp(csname);
		if (unusedfd < 0)
			pexit("Failed to generate random path for console-socket");
		close(unusedfd);

		addr.sun_family = AF_UNIX;
		strncpy(addr.sun_path, csname, sizeof(addr.sun_path)-1);

		ninfo("addr{sun_family=AF_UNIX, sun_path=%s}", addr.sun_path);

		/* Bind to the console socket path. */
		csfd = socket(AF_UNIX, SOCK_STREAM|SOCK_CLOEXEC, 0);
		if (csfd < 0)
			pexit("Failed to create console-socket");
		/* XXX: This should be handled with a rename(2). */
		if (unlink(csname) < 0)
			pexit("Failed to unlink temporary ranom path");
		if (bind(csfd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
			pexit("Failed to bind to console-socket");
		if (listen(csfd, 128) < 0)
			pexit("Failed to listen on console-socket");
	} else {
		int fds[2];

		/*
		 * Create a "fake" master fd so that we can use the same epoll code in
		 * both cases. The slavefd_*s will be closed after we dup over
		 * everything.
		 *
		 * We use pipes here because open(/dev/std{out,err}) will fail if we
		 * used anything else (and it wouldn't be a good idea to create a new
		 * pty pair in the host).
		 */
		if (pipe(fds) < 0)
			pexit("Failed to create !terminal stdout pipe");

		masterfd_stdout = fds[0];
		slavefd_stdout = fds[1];

		if (pipe(fds) < 0)
			pexit("Failed to create !terminal stderr pipe");

		masterfd_stderr = fds[0];
		slavefd_stderr = fds[1];

	}

	/* Create a pipe to attach to the container process stdin. */
	if (!terminal && !exec) {
		int fds[2];
		if (pipe(fds) < 0)
			pexit("Failed to create !terminal stderr pipe");

		masterfd_stdin = fds[0];
		slavefd_stdin = fds[1];
	}

	cmd = g_string_new(runtime_path);

	/* Generate the cmdline. */
	if (!exec && systemd_cgroup)
		g_string_append_printf(cmd, " --systemd-cgroup");

	if (exec)
		g_string_append_printf(cmd, " exec -d --pid-file %s", pid_file);
	else
		g_string_append_printf(cmd, " create --bundle %s --pid-file %s", bundle_path, pid_file);

	if (terminal)
		g_string_append_printf(cmd, " --console-socket %s", csname);

	/* Container name comes last. */
	g_string_append_printf(cmd, " %s", cid);

	/* Set the exec arguments. */
	if (exec) {
		/*
		 * FIXME: This code is broken if argv[1] contains spaces or other
		 *        similar characters that shells don't like. It's a bit silly
		 *        that we're doing things inside a shell at all -- this should
		 *        all be done in arrays.
		 */

		int i;
		for (i = 1; i < argc; i++)
			g_string_append_printf(cmd, " %s", argv[i]);
	}

	/*
	 * We have to fork here because the current runC API dups the stdio of the
	 * calling process over the container's fds. This is actually *very bad*
	 * but is currently being discussed for change in
	 * https://github.com/opencontainers/runtime-spec/pull/513. Hopefully this
	 * won't be the case for very long.
	 */

	/* Create our container. */
	create_pid = fork();
	if (create_pid < 0) {
		pexit("Failed to fork the create command");
	} else if (!create_pid) {
		char *argv[] = {"sh", "-c", cmd->str, NULL};

		/* We only need to touch the stdio if we have terminal=false. */
		/* FIXME: This results in us not outputting runc error messages to crio's log. */
		if (slavefd_stdout >= 0) {
			if (dup2(slavefd_stdout, STDOUT_FILENO) < 0)
				pexit("Failed to dup over stdout");
		}
		if (slavefd_stderr >= 0) {
			if (dup2(slavefd_stderr, STDERR_FILENO) < 0)
				pexit("Failed to dup over stderr");
		}

		if (slavefd_stdin >= 0) {
			if (dup2(slavefd_stderr, STDERR_FILENO) < 0)
				pexit("Failed to dup over stdin");
		}

		/* Exec into the process. TODO: Don't use the shell. */
		execv("/bin/sh", argv);
		exit(127);
	}

	/* The runtime has that fd now. We don't need to touch it anymore. */
	close(slavefd_stdout);
	close(slavefd_stderr);

	/* Get the console fd. */
	/*
	 * FIXME: If runc fails to start a container, we won't bail because we're
	 *        busy waiting for requests. The solution probably involves
	 *        epoll(2) and a signalfd(2). This causes a lot of issues.
	 */
	if (terminal) {
		struct file_t console;
		int connfd = -1;

		ninfo("about to accept from csfd: %d", csfd);
		connfd = accept4(csfd, NULL, NULL, SOCK_CLOEXEC);
		if (connfd < 0)
			pexit("Failed to accept console-socket connection");

		/* Not accepting anything else. */
		close(csfd);
		unlink(csname);

		/* We exit if this fails. */
		ninfo("about to recvfd from connfd: %d", connfd);
		console = recvfd(connfd);

		ninfo("console = {.name = '%s'; .fd = %d}", console.name, console.fd);
		free(console.name);

		/* We only have a single fd for both pipes, so we just treat it as
		 * stdout. stderr is ignored. */
		masterfd_stdout = console.fd;
		masterfd_stderr = -1;

		/* Clean up everything */
		close(connfd);
	}

	ninfo("about to waitpid: %d", create_pid);

	/* Wait for our create child to exit with the return code. */
	if (waitpid(create_pid, &runtime_status, 0) < 0) {
		int old_errno = errno;
		kill(create_pid, SIGKILL);
		errno = old_errno;
		pexit("Failed to wait for `runtime %s`", exec ? "exec" : "create");
	}
	if (!WIFEXITED(runtime_status) || WEXITSTATUS(runtime_status) != 0) {
		if (sync_pipe_fd > 0 && !exec) {
			if (terminal) {
				/* 
				 * For this case, the stderr is captured in the parent when terminal is passed down.
			         * We send -1 as pid to signal to parent that create container has failed.
				 */
				len = snprintf(buf, BUF_SIZE, "{\"pid\": %d}\n", -1);
				if (len < 0 || write(sync_pipe_fd, buf, len) != len) {
					pexit("unable to send container pid to parent");
				}
			} else {
				/*
				 * Read from container stderr for any error and send it to parent
			         * We send -1 as pid to signal to parent that create container has failed.
				 */
				num_read = read(masterfd_stderr, buf, BUF_SIZE);
				if (num_read > 0) {
					buf[num_read] = '\0';
					JsonGenerator *generator = json_generator_new();
					JsonNode *root;
					JsonObject *object;
					gchar *data;
					gsize len;

					root = json_node_new(JSON_NODE_OBJECT);
					object = json_object_new();

					json_object_set_int_member(object, "pid", -1);
					json_object_set_string_member(object, "message", buf);

					json_node_take_object(root, object);
					json_generator_set_root(generator, root);

					g_object_set(generator, "pretty", FALSE, NULL);
					data = json_generator_to_data (generator, &len);
					fprintf(stderr, "%s\n", data);
					if (write(sync_pipe_fd, data, len) != (int)len) {
						ninfo("Unable to send container stderr message to parent");
					}

					g_free(data);
					json_node_free(root);
					g_object_unref(generator);
				}
			}
		}
		nexit("Failed to create container: exit status %d", WEXITSTATUS(runtime_status));
	}

	/* Read the pid so we can wait for the process to exit */
	g_file_get_contents(pid_file, &contents, NULL, &err);
	if (err) {
		nwarn("Failed to read pidfile: %s", err->message);
		g_error_free(err);
		exit(1);
	}

	cpid = atoi(contents);
	ninfo("container PID: %d", cpid);

	/* Setup endpoint for attach */
	struct sockaddr_un attach_addr = {0};
	_cleanup_close_ int afd = -1;
	int asfd = -1;

	if (!exec) {
		attach_addr.sun_family = AF_UNIX;
		snprintf(attach_sock_path, PATH_MAX, "/var/run/%s-attach", cid);
		ninfo("attach sock path: %s", attach_sock_path);

		asfd = open(attach_sock_path, O_CREAT|O_WRONLY);
		if (asfd == -1) {
			pexit("Failed to create attach socket file");
		}
		close(asfd);

		strncpy(attach_addr.sun_path, attach_sock_path, sizeof(attach_addr.sun_path) - 1);
		ninfo("addr{sun_family=AF_UNIX, sun_path=%s}", attach_addr.sun_path);

		/*
		 * We make the socket non-blocking to avoid a race where client aborts connection
		 * before the server gets a chance to call accept. In that scenario, the server
		 * accept blocks till a new client connection comes in.
		 */
		afd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
		if (afd == -1)
			pexit("Failed to create attach socket");

		if (unlink(attach_sock_path) == -1 && errno != ENOENT) {
			pexit("Failed to remove socket file");
		}

		if (bind(afd, (struct sockaddr *)&attach_addr, sizeof(struct sockaddr_un)) == -1)
			pexit("Failed to bind attach socket: %s", attach_sock_path);

		if (listen(afd, 10) == -1)
			pexit("Failed to listen on attach socket: %s", attach_sock_path);
	}

	/* Setup fifo for reading in terminal resize and other stdio control messages */
	_cleanup_close_ int ctlfd = -1;
	_cleanup_close_ int dummyfd = -1;
	int ctl_msg_type = -1;
	int height = -1;
	int width = -1;
	struct winsize ws;

	snprintf(ctl_fifo_path, PATH_MAX, "%s/ctl", bundle_path);
	ninfo("ctl fifo path: %s", ctl_fifo_path);

	if (mkfifo(ctl_fifo_path, 0666) == -1)
		pexit("Failed to mkfifo at %s", ctl_fifo_path);

	ctlfd = open(ctl_fifo_path, O_RDONLY | O_NONBLOCK);
	if (ctlfd == -1)
		pexit("Failed to open control fifo");

	/*
	 * Open a dummy writer to prevent getting flood of POLLHUPs when
	 * last writer closes.
	 */
	dummyfd = open(ctl_fifo_path, O_WRONLY);
	if (dummyfd == -1)
		pexit("Failed to open dummy writer for fifo");

	ninfo("ctlfd: %d", ctlfd);

	/* Send the container pid back to parent */
	if (sync_pipe_fd > 0 && !exec) {
		len = snprintf(buf, BUF_SIZE, "{\"pid\": %d}\n", cpid);
		if (len < 0 || write(sync_pipe_fd, buf, len) != len) {
			pexit("unable to send container pid to parent");
		}
	}

	/* Setup OOM notification for container process */
	memory_cgroup_path = process_cgroup_subsystem_path(cpid, "memory");
	if (!memory_cgroup_path) {
		nexit("Failed to get memory cgroup path");
	}

	bool oom_handling_enabled = true;
	char memory_cgroup_file_path[PATH_MAX];
	snprintf(memory_cgroup_file_path, PATH_MAX, "%s/cgroup.event_control", memory_cgroup_path);
	if ((cfd = open(memory_cgroup_file_path, O_WRONLY)) == -1) {
		nwarn("Failed to open %s", memory_cgroup_file_path);
		oom_handling_enabled = false;
	}

	if (oom_handling_enabled) {
		snprintf(memory_cgroup_file_path, PATH_MAX, "%s/memory.oom_control", memory_cgroup_path);
		if ((ofd = open(memory_cgroup_file_path, O_RDONLY)) == -1)
			pexit("Failed to open %s", memory_cgroup_file_path);

		if ((efd = eventfd(0, 0)) == -1)
			pexit("Failed to create eventfd");

		wb = snprintf(buf, BUF_SIZE, "%d %d", efd, ofd);
		if (write(cfd, buf, wb) < 0)
			pexit("Failed to write to cgroup.event_control");
	}

	/* Create epoll_ctl so that we can handle read/write events. */
	/*
	 * TODO: Switch to libuv so that we can also implement exec as well as
	 *       attach and other important things. Using epoll directly is just
	 *       really nasty.
	 */
	epfd = epoll_create(5);
	if (epfd < 0)
		pexit("epoll_create");
	ev.events = EPOLLIN;
	if (masterfd_stdout >= 0) {
		ev.data.fd = masterfd_stdout;
		if (epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev) < 0)
			pexit("Failed to add console masterfd_stdout to epoll");
		num_stdio_fds++;
	}
	if (masterfd_stderr >= 0) {
		ev.data.fd = masterfd_stderr;
		if (epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev) < 0)
			pexit("Failed to add console masterfd_stderr to epoll");
		num_stdio_fds++;
	}

	/* Add the OOM event fd to epoll */
	if (oom_handling_enabled) {
		ev.data.fd = efd;
		if (epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev) < 0)
			pexit("Failed to add OOM eventfd to epoll");
	}

	/* Add the attach socket to epoll */
	if (afd > 0) {
		ev.data.fd = afd;
		if (epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev) < 0)
			pexit("Failed to add attach socket fd to epoll");
	}

	/* Add control fifo fd to epoll */
	ev.data.fd = ctlfd;
	if (epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev) < 0)
		pexit("Failed to add control fifo fd to epoll");

	/* Log all of the container's output. */
	while (num_stdio_fds > 0) {
		int ready = epoll_wait(epfd, evlist, MAX_EVENTS, -1);
		if (ready < 0)
			continue;

		for (int i = 0; i < ready; i++) {
			if (evlist[i].events & EPOLLIN) {
				int masterfd = evlist[i].data.fd;
				stdpipe_t pipe;
				if (masterfd == masterfd_stdout)
					pipe = STDOUT_PIPE;
				else if (masterfd == masterfd_stderr)
					pipe = STDERR_PIPE;
				else if (oom_handling_enabled && masterfd == efd) {
					if (read(efd, &oom_event, sizeof(uint64_t)) != sizeof(uint64_t))
						nwarn("Failed to read event from eventfd");
					ninfo("OOM received");
					if (open("oom", O_CREAT, 0666) < 0) {
						nwarn("Failed to write oom file");
					}
				} else if (evlist[i].data.fd == afd) {
					conn_sock = accept(afd, NULL, NULL);
					if (conn_sock == -1) {
						nwarn("Failed to accept client connection on attach socket");
						continue;
					}
					ev.events = EPOLLIN;
					ev.data.fd = conn_sock;
					if (epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev) == -1) {
						pexit("Failed to add client socket fd to epoll");
					}
					ninfo("Accepted connection");
				} else if (evlist[i].data.fd == ctlfd) {
					num_read = read(ctlfd, buf, BUF_SIZE);
					if (num_read <= 0) {
						nwarn("Failed to read from control fd");
						continue;
					}
					buf[num_read] = '\0';
					ninfo("Got ctl message: %s\n", buf);
					ret = sscanf(buf, "%d %d %d\n", &ctl_msg_type, &height, &width);
					if (ret != 3) {
						nwarn("Failed to sscanf message");
						continue;
					}
					ninfo("Message type: %d, Height: %d, Width: %d", ctl_msg_type, height, width);
					if (terminal) {
						ws.ws_row = height;
						ws.ws_col = width;
						ret = ioctl(masterfd_stdout, TIOCSWINSZ, &ws);
						if (ret == -1) {
							pexit("Failed to set process pty terminal size");
						}
					}
				} else {
					num_read = read(masterfd, buf, BUF_SIZE);
					if (num_read <= 0)
						goto out;
					ninfo("got data on connection: %d", num_read);
					if (terminal) {
						if (write(masterfd_stdout, buf, num_read) < 0) {
							nwarn("failed to write to master pty");
						}
						ninfo("Wrote to master pty");
					}
				}

				if (masterfd == masterfd_stdout || masterfd == masterfd_stderr) {
					num_read = read(masterfd, buf, BUF_SIZE);
					if (num_read <= 0)
						goto out;

					if (write_k8s_log(logfd, pipe, buf, num_read) < 0) {
						nwarn("write_k8s_log failed");
						goto out;
					}

					if (conn_sock > 0) {
						if (write(conn_sock, buf, num_read) < 0) {
							nwarn("failed to write to socket");
						}
						ninfo("Wrote to client");
					}
				}
			} else if (evlist[i].events & (EPOLLHUP | EPOLLERR)) {
				if (evlist[i].data.fd == ctlfd) {
					ninfo("Remote writer to control fd close");
					continue;
				}
				printf("closing fd %d\n", evlist[i].data.fd);
				if (close(evlist[i].data.fd) < 0)
					pexit("close");
				num_stdio_fds--;
			}
		}
	}

out:
	/* Wait for the container process and record its exit code */
	while ((pid = waitpid(-1, &status, 0)) > 0) {
		int exit_status = WEXITSTATUS(status);

		printf("PID %d exited with status %d\n", pid, exit_status);
		if (pid == cpid) {
			if (!exec) {
				_cleanup_free_ char *status_str = NULL;
				ret = asprintf(&status_str, "%d", exit_status);
				if (ret < 0) {
					pexit("Failed to allocate memory for status");
				}
				g_file_set_contents("exit", status_str,
						    strlen(status_str), &err);
				if (err) {
					fprintf(stderr,
						"Failed to write %s to exit file: %s\n",
						status_str, err->message);
					g_error_free(err);
					exit(1);
				}
			} else {
				/* Send the command exec exit code back to the parent */
				if (sync_pipe_fd > 0) {
					len = snprintf(buf, BUF_SIZE, "{\"exit_code\": %d}\n", exit_status);
					if (len < 0 || write(sync_pipe_fd, buf, len) != len) {
						pexit("unable to send exit status");
						exit(1);
					}
				}
			}
			break;
		}
	}

	if (exec && pid < 0 && errno == ECHILD && sync_pipe_fd > 0) {
		/*
		 * waitpid failed and set errno to ECHILD:
		 * The runtime exec call did not create any child
		 * process and we can send the system() exit code
		 * to the parent.
		 */
		len = snprintf(buf, BUF_SIZE, "{\"exit_code\": %d}\n", WEXITSTATUS(runtime_status));
		if (len < 0 || write(sync_pipe_fd, buf, len) != len) {
			pexit("unable to send exit status");
			exit(1);
		}
	}

	if (!exec) {
		if (unlink(attach_sock_path) == -1 && errno != ENOENT) {
			pexit("Failed to remove socket file");
		}
	}

	return EXIT_SUCCESS;
}
