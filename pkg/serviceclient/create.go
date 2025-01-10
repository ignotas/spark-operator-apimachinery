package serviceclient

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"unicode/utf8"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/ignotas/spark-operator-apimachinery/api/v1beta2"
)

type blobHandler interface {
	// TODO: With go-cloud supporting setting ACLs, remove implementations of interface
	setPublicACL(ctx context.Context, bucket string, filePath string) error
}

type uploadHandler struct {
	blob             blobHandler
	blobUploadBucket string
	blobEndpoint     string
	hdpScheme        string
	ctx              context.Context
	b                *blob.Bucket
}

func (uh uploadHandler) uploadToBucket(uploadPath, localFilePath string, Override bool, Public bool) (string, error) {
	fileName := filepath.Base(localFilePath)
	uploadFilePath := filepath.Join(uploadPath, fileName)

	// Check if exists by trying to fetch metadata
	reader, err := uh.b.NewRangeReader(uh.ctx, uploadFilePath, 0, 0, nil)
	if err == nil {
		reader.Close()
	}
	if (gcerrors.Code(err) == gcerrors.NotFound) || (err == nil && Override) {
		fmt.Printf("uploading local file: %s\n", fileName)

		// Prepare the file for upload.
		data, err := os.ReadFile(localFilePath)
		if err != nil {
			return "", fmt.Errorf("failed to read file: %s", err)
		}

		// Open Bucket
		w, err := uh.b.NewWriter(uh.ctx, uploadFilePath, nil)
		if err != nil {
			return "", fmt.Errorf("failed to obtain bucket writer: %s", err)
		}

		// Write data to bucket and close bucket writer
		_, writeErr := w.Write(data)
		if err := w.Close(); err != nil {
			return "", fmt.Errorf("failed to close bucket writer: %s", err)
		}

		// Check if write has been successful
		if writeErr != nil {
			return "", fmt.Errorf("failed to write to bucket: %s", err)
		}

		// Set public ACL if needed
		if Public {
			err := uh.blob.setPublicACL(uh.ctx, uh.blobUploadBucket, uploadFilePath)
			if err != nil {
				return "", err
			}

			endpointURL, err := url.Parse(uh.blobEndpoint)
			if err != nil {
				return "", err
			}
			// Public needs full bucket endpoint
			return fmt.Sprintf("%s://%s/%s/%s",
				endpointURL.Scheme,
				endpointURL.Host,
				uh.blobUploadBucket,
				uploadFilePath), nil
		}
	} else if err == nil {
		fmt.Printf("not uploading file %s as it already exists remotely\n", fileName)
	} else {
		return "", err
	}
	// Return path to file with proper hadoop-connector scheme
	return fmt.Sprintf("%s://%s/%s", uh.hdpScheme, uh.blobUploadBucket, uploadFilePath), nil
}

func uploadLocalDependencies(ctx context.Context, app *v1beta2.SparkApplication, files []string, uploadToPath string, UploadToEndpoint string, UploadToRegion string, S3ForcePathStyle bool, RootPath string, Override bool, Public bool) ([]string, error) {
	if uploadToPath == "" {
		return nil, fmt.Errorf(
			"unable to upload local dependencies: no upload location specified via --upload-to")
	}

	uploadLocationURL, err := url.Parse(uploadToPath)
	if err != nil {
		return nil, err
	}
	uploadBucket := uploadLocationURL.Host

	var uh *uploadHandler

	switch uploadLocationURL.Scheme {
	case "gs":
		uh, err = newGCSBlob(ctx, uploadBucket, UploadToEndpoint, UploadToRegion)
	case "s3":
		uh, err = newS3Blob(ctx, uploadBucket, UploadToEndpoint, UploadToRegion, S3ForcePathStyle)
	default:
		return nil, fmt.Errorf("unsupported upload location URL scheme: %s", uploadLocationURL.Scheme)
	}

	// Check if bucket has been successfully setup
	if err != nil {
		return nil, err
	}

	var uploadedFilePaths []string
	uploadPath := filepath.Join(RootPath, app.Namespace, app.Name)
	for _, localFilePath := range files {
		uploadFilePath, err := uh.uploadToBucket(uploadPath, localFilePath, Override, Public)
		if err != nil {
			return nil, err
		}

		uploadedFilePaths = append(uploadedFilePaths, uploadFilePath)
	}

	return uploadedFilePaths, nil
}

func handleHadoopConfiguration(
	Namespace string,
	app *v1beta2.SparkApplication,
	hadoopConfDir string,
	kubeClientset clientset.Interface) error {
	configMap, err := buildHadoopConfigMap(Namespace, app.Name, hadoopConfDir)
	if err != nil {
		return fmt.Errorf("failed to create a ConfigMap for Hadoop configuration files in %s: %v",
			hadoopConfDir, err)
	}

	err = kubeClientset.CoreV1().ConfigMaps(Namespace).Delete(context.TODO(), configMap.Name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete existing ConfigMap %s: %v", configMap.Name, err)
	}

	if configMap, err = kubeClientset.CoreV1().ConfigMaps(Namespace).Create(context.TODO(), configMap, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create ConfigMap %s: %v", configMap.Name, err)
	}

	app.Spec.HadoopConfigMap = &configMap.Name

	return nil
}

func buildHadoopConfigMap(Namespace string, appName string, hadoopConfDir string) (*corev1.ConfigMap, error) {
	info, err := os.Stat(hadoopConfDir)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", hadoopConfDir)
	}

	files, err := os.ReadDir(hadoopConfDir)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no Hadoop configuration file found in %s", hadoopConfDir)
	}

	hadoopStringConfigFiles := make(map[string]string)
	hadoopBinaryConfigFiles := make(map[string][]byte)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		content, err := os.ReadFile(filepath.Join(hadoopConfDir, file.Name()))
		if err != nil {
			return nil, err
		}

		if utf8.Valid(content) {
			hadoopStringConfigFiles[file.Name()] = string(content)
		} else {
			hadoopBinaryConfigFiles[file.Name()] = content
		}
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName + "-hadoop-config",
			Namespace: Namespace,
		},
		Data:       hadoopStringConfigFiles,
		BinaryData: hadoopBinaryConfigFiles,
	}

	return configMap, nil
}

func handleLocalDependencies(ctx context.Context, app *v1beta2.SparkApplication, uploadToPath string, UploadToEndpoint string, UploadToRegion string, S3ForcePathStyle bool, RootPath string, Override bool, Public bool) error {
	if app.Spec.MainApplicationFile != nil {
		isMainAppFileLocal, err := isLocalFile(*app.Spec.MainApplicationFile)
		if err != nil {
			return err
		}

		if isMainAppFileLocal {
			uploadedMainFile, err := uploadLocalDependencies(ctx, app, []string{*app.Spec.MainApplicationFile}, uploadToPath, UploadToEndpoint, UploadToRegion, S3ForcePathStyle, RootPath, Override, Public)
			if err != nil {
				return fmt.Errorf("failed to upload local main application file: %v", err)
			}
			app.Spec.MainApplicationFile = &uploadedMainFile[0]
		}
	}

	localJars, err := filterLocalFiles(app.Spec.Deps.Jars)
	if err != nil {
		return fmt.Errorf("failed to filter local jars: %v", err)
	}

	if len(localJars) > 0 {
		uploadedJars, err := uploadLocalDependencies(ctx, app, localJars, uploadToPath, UploadToEndpoint, UploadToRegion, S3ForcePathStyle, RootPath, Override, Public)
		if err != nil {
			return fmt.Errorf("failed to upload local jars: %v", err)
		}
		app.Spec.Deps.Jars = uploadedJars
	}

	localFiles, err := filterLocalFiles(app.Spec.Deps.Files)
	if err != nil {
		return fmt.Errorf("failed to filter local files: %v", err)
	}

	if len(localFiles) > 0 {
		uploadedFiles, err := uploadLocalDependencies(ctx, app, localFiles, uploadToPath, UploadToEndpoint, UploadToRegion, S3ForcePathStyle, RootPath, Override, Public)
		if err != nil {
			return fmt.Errorf("failed to upload local files: %v", err)
		}
		app.Spec.Deps.Files = uploadedFiles
	}

	localPyFiles, err := filterLocalFiles(app.Spec.Deps.PyFiles)
	if err != nil {
		return fmt.Errorf("failed to filter local pyfiles: %v", err)
	}

	if len(localPyFiles) > 0 {
		uploadedPyFiles, err := uploadLocalDependencies(ctx, app, localPyFiles, uploadToPath, UploadToEndpoint, UploadToRegion, S3ForcePathStyle, RootPath, Override, Public)
		if err != nil {
			return fmt.Errorf("failed to upload local pyfiles: %v", err)
		}
		app.Spec.Deps.PyFiles = uploadedPyFiles
	}

	return nil
}

func (c *sparkClient) CreateSparkApplication(ctx context.Context, app *v1beta2.SparkApplication, DeleteIfExists bool, Namespace string, UploadToPath string, UploadToEndpoint string, UploadToRegion string, S3ForcePathStyle bool, RootPath string, Override bool, Public bool) error {
	if DeleteIfExists {
		if err := c.DeleteSparkApplication(ctx, Namespace, app.Name); err != nil {
			return err
		}
	}

	v1beta2.SetSparkApplicationDefaults(app)
	if err := validateSpec(app.Spec); err != nil {
		return err
	}

	if err := handleLocalDependencies(ctx, app, UploadToPath, UploadToEndpoint, UploadToRegion, S3ForcePathStyle, RootPath, Override, Public); err != nil {
		return err
	}

	if hadoopConfDir := os.Getenv("HADOOP_CONF_DIR"); hadoopConfDir != "" {
		fmt.Println("creating a ConfigMap for Hadoop configuration files in HADOOP_CONF_DIR")
		if err := handleHadoopConfiguration(Namespace, app, hadoopConfDir, c.kubeClient); err != nil {
			return err
		}
	}

	if _, err := c.crdClientSet.SparkoperatorV1beta2().SparkApplications(Namespace).Create(
		context.TODO(),
		app,
		metav1.CreateOptions{},
	); err != nil {
		return err
	}

	return nil
}

func filterLocalFiles(files []string) ([]string, error) {
	var localFiles []string
	for _, file := range files {
		if isLocal, err := isLocalFile(file); err != nil {
			return nil, err
		} else if isLocal {
			localFiles = append(localFiles, file)
		}
	}

	return localFiles, nil
}

func isLocalFile(file string) (bool, error) {
	fileURL, err := url.Parse(file)
	if err != nil {
		return false, err
	}

	if fileURL.Scheme == "file" || fileURL.Scheme == "" {
		return true, nil
	}

	return false, nil
}

func validateSpec(spec v1beta2.SparkApplicationSpec) error {
	if spec.Image == nil && (spec.Driver.Image == nil || spec.Executor.Image == nil) {
		return fmt.Errorf("'spec.driver.image' and 'spec.executor.image' cannot be empty when 'spec.image' " +
			"is not set")
	}

	return nil
}
