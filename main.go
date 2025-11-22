package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func main() {
	bucket := os.Getenv("BUCKET_NAME")
	endpoint := os.Getenv("S3_ENDPOINT")
	access := os.Getenv("S3_ACCESS_KEY")
	secret := os.Getenv("S3_SECRET_KEY")
	region := os.Getenv("S3_REGION")

	if bucket == "" || endpoint == "" || access == "" || secret == "" {
		log.Fatal("Fehlen Env Vars: BUCKET_NAME, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY")
	}

	if region == "" {
		region = "us-east-1"
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, "")),
	)
	if err != nil {
		log.Fatalf("Fehler beim Laden der Config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFromURL(endpoint)
		o.UsePathStyle = true
	})

	ctx := context.Background()

	blockPrefixes, err := listBlocks(ctx, client, bucket, "anonymous/")
	if err != nil {
		log.Fatalf("Fehler beim Auflisten der Blöcke: %v", err)
	}

	for _, block := range blockPrefixes {
		fmt.Printf("Prüfe Block: %s\n", block)

		hasIndexHeader, err := checkObjectExists(ctx, client, bucket, filepath.Join(block, "index-header"))
		if err != nil {
			log.Printf("Fehler beim Prüfen index-header: %v", err)
			continue
		}

		if hasIndexHeader {
			fmt.Printf("  index-header existiert bereits, überspringe\n")
			continue
		}

		fmt.Printf("  index-header fehlt → erzeugen...\n")
		headerData := []byte("dummy index-header content")

		err = uploadObject(ctx, client, bucket, filepath.Join(block, "index-header"), headerData)
		if err != nil {
			log.Printf("Fehler beim Hochladen index-header: %v", err)
			continue
		}

		fmt.Printf("  index-header hochgeladen\n")
	}

	fmt.Println("Fertig! Alle fehlenden index-header wurden erstellt.")
}

// listBlocks listet direkte Unterordner unter prefix
func listBlocks(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var blocks []string

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, cp := range page.CommonPrefixes {
			if cp.Prefix != nil {
				blocks = append(blocks, *cp.Prefix)
			}
		}
	}

	return blocks, nil
}

func checkObjectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &apiErr) && apiErr.Code == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// uploadObject lädt ein Objekt nach S3 hoch
func uploadObject(ctx context.Context, client *s3.Client, bucket, key string, data []byte) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

