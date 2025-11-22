package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	bucket := os.Getenv("BUCKET_NAME")
	endpoint := os.Getenv("S3_ENDPOINT")
	region := os.Getenv("AWS_REGION")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")

	if bucket == "" || endpoint == "" || accessKey == "" || secretKey == "" {
		log.Fatal("Bitte BUCKET_NAME, S3_ENDPOINT, S3_ACCESS_KEY und S3_SECRET_KEY als Env-Variablen setzen")
	}

	if region == "" {
		region = "us-east-1"
	}

	// AWS Config mit Env-Variablen
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.Fatalf("Fehler beim Laden der Config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFromURL(endpoint)
	})

	ctx := context.Background()

	// Alle Block-Ordner auflisten
	prefix := ""
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	fmt.Println("Listing blocks...")

	var blocks []string

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("Fehler beim Listen von S3: %v", err)
		}
		for _, commonPrefix := range page.CommonPrefixes {
			blocks = append(blocks, *commonPrefix.Prefix)
		}
	}

	fmt.Printf("Gefundene Blöcke: %d\n", len(blocks))

	// Jeden Block "markieren" -> index-header umbenennen/löschen
	for _, block := range blocks {
		fmt.Printf("Bearbeite Block: %s\n", block)

		indexHeaderKey := path.Join(block, "index-header")
		newKey := path.Join(block, "index-header.old")

		// Kopieren
		// Kopieren
		_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			CopySource: aws.String(bucket + "/" + indexHeaderKey),
			Key:        aws.String(newKey),
		})
		if err != nil {
			if _, ok := err.(*types.NoSuchKey); ok {
				fmt.Printf("Kein index-header für Block %s, überspringe\n", block)
				continue
			}
			log.Printf("Fehler beim Kopieren von %s: %v\n", indexHeaderKey, err)
			continue
		}
		// Löschen, damit Compactor neue Header schreibt
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(indexHeaderKey),
		})
		if err != nil {
			log.Printf("Fehler beim Löschen von %s: %v\n", indexHeaderKey, err)
			continue
		}

		fmt.Printf("Block %s markiert für Header-Recompaction\n", block)
	}

	fmt.Println("Fertig! Jetzt den Compactor starten, um neue Sparse Index-Header zu erzeugen.")
}
