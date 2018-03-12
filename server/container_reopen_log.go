package server

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kubernetes-incubator/cri-o/oci"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
	pb "k8s.io/kubernetes/pkg/kubelet/apis/cri/runtime/v1alpha2"
)

// ReopenContainerLog reopens the containers log file
func (s *Server) ReopenContainerLog(ctx context.Context, req *pb.ReopenContainerLogRequest) (resp *pb.ReopenContainerLogResponse, err error) {
	const operation = "container_reopen_log"
	defer func() {
		recordOperation(operation, time.Now())
		recordError(operation, err)
	}()

	logrus.Debugf("ReopenContainerLogRequest %+v", req)
	containerID := req.ContainerId
	c := s.GetContainer(containerID)

	if c == nil {
		return nil, fmt.Errorf("could not find container %q", containerID)
	}

	if err := s.ContainerServer.Runtime().UpdateStatus(c); err != nil {
		return nil, err
	}

	cState := s.ContainerServer.Runtime().ContainerStatus(c)
	if !(cState.Status == oci.ContainerStateRunning || cState.Status == oci.ContainerStateCreated) {
		return nil, fmt.Errorf("container is not created or running")
	}

	controlPath := filepath.Join(c.BundlePath(), "ctl")
	controlFile, err := os.OpenFile(controlPath, unix.O_WRONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open container ctl file: %v", err)
	}
	defer controlFile.Close()

	_, err = fmt.Fprintf(controlFile, "%d %d %d\n", 2, 0, 0)
	if err != nil {
		logrus.Infof("Failed to write to control file to reopen log file: %v", err)
	}

	resp = &pb.ReopenContainerLogResponse{}
	logrus.Debugf("ReopenContainerLogResponse %s: %+v", containerID, resp)
	return resp, nil
}
