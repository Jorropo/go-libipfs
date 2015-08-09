package tar

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"io"
	"path"
	"time"

	proto "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/gogo/protobuf/proto"
	cxt "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"

	mdag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"
	uio "github.com/ipfs/go-ipfs/unixfs/io"
	upb "github.com/ipfs/go-ipfs/unixfs/pb"
)

// DefaultBufSize is the buffer size for gets. for now, 1MB, which is ~4 blocks.
// TODO: does this need to be configurable?
var DefaultBufSize = 1048576

// DagArchive is equivalent to `ipfs getdag $hash | maybe_tar | maybe_gzip`
func DagArchive(ctx cxt.Context, nd *mdag.Node, name string, dag mdag.DAGService, archive bool, compression int) (io.Reader, error) {

	_, filename := path.Split(name)

	// need to connect a writer to a reader
	piper, pipew := io.Pipe()

	// use a buffered writer to parallelize task
	bufw := bufio.NewWriterSize(pipew, DefaultBufSize)

	// compression determines whether to use gzip compression.
	var maybeGzw io.Writer
	if compression != gzip.NoCompression {
		var err error
		maybeGzw, err = gzip.NewWriterLevel(bufw, compression)
		if err != nil {
			return nil, err
		}
	} else {
		maybeGzw = bufw
	}

	// construct the tar writer
	w, err := NewWriter(ctx, dag, archive, compression, maybeGzw)
	if err != nil {
		return nil, err
	}

	// write all the nodes recursively
	go func() {
		if !archive && compression != gzip.NoCompression {
			// the case when the node is a file
			dagr, err := uio.NewDagReader(w.ctx, nd, w.Dag)
			if err != nil {
				pipew.CloseWithError(err)
				return
			}

			if _, err := dagr.WriteTo(maybeGzw); err != nil {
				pipew.CloseWithError(err)
				return
			}
		} else {
			// the case for 1. archive, and 2. not archived and not compressed, in which tar is used anyway as a transport format
			if err := w.WriteNode(nd, filename); err != nil {
				pipew.CloseWithError(err)
				return
			}
		}

		if err := bufw.Flush(); err != nil {
			pipew.CloseWithError(err)
			return
		}

		w.Close()
		pipew.Close() // everything seems to be ok.
	}()

	return piper, nil
}

// Writer is a utility structure that helps to write
// unixfs merkledag nodes as a tar archive format.
// It wraps any io.Writer.
type Writer struct {
	Dag  mdag.DAGService
	TarW *tar.Writer

	ctx cxt.Context
}

// NewWriter wraps given io.Writer.
func NewWriter(ctx cxt.Context, dag mdag.DAGService, archive bool, compression int, w io.Writer) (*Writer, error) {
	return &Writer{
		Dag:  dag,
		TarW: tar.NewWriter(w),
		ctx:  ctx,
	}, nil
}

func (w *Writer) writeDir(nd *mdag.Node, fpath string) error {
	if err := writeDirHeader(w.TarW, fpath); err != nil {
		return err
	}

	for i, ng := range w.Dag.GetDAG(w.ctx, nd) {
		child, err := ng.Get(w.ctx)
		if err != nil {
			return err
		}

		npath := path.Join(fpath, nd.Links[i].Name)
		if err := w.WriteNode(child, npath); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeFile(nd *mdag.Node, pb *upb.Data, fpath string) error {
	if err := writeFileHeader(w.TarW, fpath, pb.GetFilesize()); err != nil {
		return err
	}

	dagr := uio.NewDataFileReader(w.ctx, nd, pb, w.Dag)
	_, err := dagr.WriteTo(w.TarW)
	return err
}

func (w *Writer) WriteNode(nd *mdag.Node, fpath string) error {
	pb := new(upb.Data)
	if err := proto.Unmarshal(nd.Data, pb); err != nil {
		return err
	}

	switch pb.GetType() {
	case upb.Data_Metadata:
		fallthrough
	case upb.Data_Directory:
		return w.writeDir(nd, fpath)
	case upb.Data_Raw:
		fallthrough
	case upb.Data_File:
		return w.writeFile(nd, pb, fpath)
	default:
		return ft.ErrUnrecognizedType
	}
}

func (w *Writer) Close() error {
	return w.TarW.Close()
}

func writeDirHeader(w *tar.Writer, fpath string) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Typeflag: tar.TypeDir,
		Mode:     0777,
		ModTime:  time.Now(),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}

func writeFileHeader(w *tar.Writer, fpath string, size uint64) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Size:     int64(size),
		Typeflag: tar.TypeReg,
		Mode:     0644,
		ModTime:  time.Now(),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}
