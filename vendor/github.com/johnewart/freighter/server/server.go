package server

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"net"
	"os"

	pb "github.com/johnewart/freighter/freighter/proto"
	"google.golang.org/grpc"
	"zombiezen.com/go/log"
)

type server struct {
	pb.UnimplementedFreighterServer
	LayerRootPath string
}

func (s *server) LayerFilePath(layer string) string {
	root := strings.TrimRight(s.LayerRootPath, "/")
	return fmt.Sprintf("%s/%s", root, layer)
}

func (s *server) GetFile(ctx context.Context, in *pb.FileRequest) (*pb.FileReply, error) {
	tarFileName := s.LayerFilePath(in.GetContainerId())
	log.Infof(ctx, "Reading '%s' from %s", in.GetPath(), tarFileName)
	if data, err := ReadFileFromTar(tarFileName, in.GetPath()); err != nil {
		log.Errorf(ctx, "Error reading file: %v", err)
		return nil, err
	} else {
		return &pb.FileReply{Data: data}, nil
	}
}

func (s *server) GetDir(ctx context.Context, in *pb.DirRequest) (*pb.DirReply, error) {
	log.Infof(ctx, "Received: %v", in.GetPath())

	files := make([]*pb.FileInfo, 0)

	if in.GetPath() == "" {
		if direntries, err := os.ReadDir(s.LayerRootPath); err != nil {
			log.Errorf(ctx, "Error reading directory: %v", err)
		} else {
			for _, entry := range direntries {
				info, err := entry.Info()
				var fsize int64
				if err != nil {
					log.Errorf(ctx, "Error reading file info: %v", err)
					fsize = 0
				}
				fsize = int64(info.Size())

				files = append(files, &pb.FileInfo{Name: entry.Name(), Size: fsize, IsDir: true})
			}
		}
	} else {
		fname := s.LayerFilePath(in.GetPath())
		log.Infof(ctx, "Reading from %s", fname)
		if file, err := os.Open(fname); err == nil {
			archive, err := gzip.NewReader(file)

			if err != nil {
				log.Errorf(ctx, "Error reading gzip: %v", err)
			} else {

				tr := tar.NewReader(archive)

				if tr == nil {
					log.Errorf(ctx, "Error reading tar...")
				} else {
					for {
						hdr, err := tr.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							log.Errorf(ctx, "Error reading tar: %v", err)
							continue
						}
						files = append(files, &pb.FileInfo{Name: hdr.Name, Size: hdr.Size, IsDir: false})
					}
				}
			}
		}
	}

	return &pb.DirReply{Files: files}, nil

}

func ReadFileFromTar(tarFile string, filename string) ([]byte, error) {
	file, err := os.Open(tarFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	archive, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer archive.Close()

	tr := tar.NewReader(archive)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Name == filename {
			buf, err := ioutil.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			return buf, nil
		}
	}
	return nil, fmt.Errorf("File not found: %s", filename)
}

type FreighterServer struct {
	server *grpc.Server
	ctx    context.Context
}

func NewFreighterServer(rootPath string) *FreighterServer {
	ctx := context.Background()

	s := grpc.NewServer()
	pb.RegisterFreighterServer(s, &server{
		LayerRootPath: rootPath,
	})

	log.Infof(ctx, "Registering Freighter server with layer root at %s", rootPath)

	return &FreighterServer{
		ctx:    ctx,
		server: s,
	}
}

func (fs *FreighterServer) Serve(host string, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Errorf(fs.ctx, "failed to listen: %v", err)
		os.Exit(1)
	}
	log.Infof(fs.ctx, "server listening at %v", lis.Addr())
	if err := fs.server.Serve(lis); err != nil {
		log.Errorf(fs.ctx, "failed to serve: %v", err)
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
