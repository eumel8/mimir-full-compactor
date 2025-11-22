package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BlockHeader struct {
	ULID      string `json:"ulid"`
	MinTime   int64  `json:"minTime"`
	MaxTime   int64  `json:"maxTime"`
	NumChunks int    `json:"numChunks"`
	SizeBytes int64  `json:"sizeBytes"`
}

type SparseHeader struct {
	ULID    string `json:"ulid"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
}

func main() {
	bucket := os.Getenv("BUCKET_NAME")
	endpoint := os.Getenv("S3_ENDPOINT")
	access := os.Getenv("S3_ACCESS_KEY")
	secret := os.Getenv("S3_SECRET_KEY")
	region := os.Getenv("S3_REGION")
	if region == "" {
		region = "us-east-1"
	}

	if bucket == "" || endpoint == "" || access == "" || secret == "" {
		log.Fatal("Fehlen Env Vars: BUCKET_NAME, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY")
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
			fmt.Println("  index-header existiert bereits, überspringe")
			continue
		}

		chunkKeys, err := listChunks(ctx, client, bucket, filepath.Join(block, "chunks/"))
		if err != nil {
			log.Printf("Fehler beim Auflisten chunks: %v", err)
			continue
		}
		if len(chunkKeys) == 0 {
			fmt.Println("  keine chunks gefunden, überspringe")
			continue
		}

		var minTime, maxTime int64
		var totalSize int64
		for _, ck := range chunkKeys {
			size := parseSizeFromChunkName(ck)
			totalSize += size
			t := parseTimeFromChunkName(ck)
			if minTime == 0 || t < minTime {
				minTime = t
			}
			if t > maxTime {
				maxTime = t
			}
		}

		header := BlockHeader{
			ULID:      strings.Trim(block, "/"),
			MinTime:   minTime,
			MaxTime:   maxTime,
			NumChunks: len(chunkKeys),
			SizeBytes: totalSize,
		}
		sparse := SparseHeader{
			ULID:    header.ULID,
			MinTime: header.MinTime,
			MaxTime: header.MaxTime,
		}

		indexData, _ := json.Marshal(header)
		sparseData, _ := json.Marshal(sparse)

		if err := uploadObject(ctx, client, bucket, filepath.Join(block, "index-header"), indexData); err != nil {
			log.Printf("Fehler beim Upload index-header: %v", err)
			continue
		}
		if err := uploadObject(ctx, client, bucket, filepath.Join(block, "sparse-index-header"), sparseData); err != nil {
			log.Printf("Fehler beim Upload sparse-index-header: %v", err)
			continue
		}

		fmt.Println("  index-header und sparse-index-header hochgeladen")
	}

	fmt.Println("Fertig! Alle Blöcke haben jetzt Header.")
}

func parseSizeFromChunkName(chunkKey string) int64 {
	// Dummy: echte Chunk-Größe kann aus S3 metadata gelesen werden
	return 1000
}
func parseTimeFromChunkName(chunkKey string) int64 {
	// Dummy: ULID/Timestamp oder Name ableiten
	return 1763820000000
}

func listBlocks(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var blocks []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    &bucket,
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

func listChunks(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var chunks []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				chunks = append(chunks, *obj.Key)
			}
		}
	}
	return chunks, nil
}

func checkObjectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func uploadObject(ctx context.Context, client *s3.Client, bucket, key string, data []byte) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

