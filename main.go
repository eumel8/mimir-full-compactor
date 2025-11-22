package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	//"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/config"
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
		region = "eu-central-1"
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
		o.UsePathStyle = true // ⚠️ wichtig für OEM-S3
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

	// Parallelisierung
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // max 10 parallele Requests

	for _, block := range blocks {
		wg.Add(1)
		go func(block string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			indexHeaderKey := path.Join(block, "index-header")
			newKey := path.Join(block, "index-header.old")

			// Prüfen, ob index-header existiert
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(indexHeaderKey),
			})
			if err != nil {
				// var nf *types.NotFound
				if ok := err != nil; ok {
					fmt.Printf("Kein index-header für Block %s, überspringe\n", block)
					return
				}
			}

			// Retry-Funktion für Copy & Delete
			retry := func(f func() error) error {
				for i := 0; i < 3; i++ {
					if err := f(); err != nil {
						fmt.Printf("Fehler: %v, retry %d/3\n", err, i+1)
						time.Sleep(2 * time.Second)
					} else {
						return nil
					}
				}
				return fmt.Errorf("Operation nach 3 Versuchen fehlgeschlagen")
			}

			// Copy index-header
			err = retry(func() error {
				_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
					Bucket:     aws.String(bucket),
					CopySource: aws.String(bucket + "/" + indexHeaderKey),
					Key:        aws.String(newKey),
				})
				return err
			})
			if err != nil {
				fmt.Printf("WARN: Konnte %s nicht kopieren: %v\n", indexHeaderKey, err)
				return
			}

			// Delete original
			err = retry(func() error {
				_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(indexHeaderKey),
				})
				return err
			})
			if err != nil {
				fmt.Printf("WARN: Konnte %s nicht löschen: %v\n", indexHeaderKey, err)
				return
			}

			fmt.Printf("Block %s markiert für Header-Recompaction\n", block)
		}(block)
	}

	wg.Wait()
	fmt.Println("Fertig! Jetzt den Compactor starten, um neue Sparse Index-Header zu erzeugen.")
}

