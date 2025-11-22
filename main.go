package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	bucket := os.Getenv("BUCKET_NAME")
	endpoint := os.Getenv("S3_ENDPOINT")
	access := os.Getenv("S3_ACCESS_KEY")
	secret := os.Getenv("S3_SECRET_KEY")

	if bucket == "" || endpoint == "" || access == "" || secret == "" {
		log.Fatal("Fehlen Env Vars: BUCKET_NAME, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, "")),
		config.WithRegion("eu-central-1"),
	)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFromURL(endpoint)
		o.UsePathStyle = true
	})

	ctx := context.Background()

	fmt.Println("=== Scanne Bucket vollständig nach meta.json ===")

	// meta.json-Erkennung
	reMeta := regexp.MustCompile(`^(.*\/)?([0-9A-Za-z]{12,})\/meta\.json$`)

	foundBlocks := map[string]string{} // blockID → prefix

	var token *string

	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
		})
		if err != nil {
			log.Fatalf("ListObjectsV2 Fehler: %v", err)
		}

		for _, obj := range out.Contents {
			key := *obj.Key

			fmt.Printf("Gefundenes Objekt: %s\n", key)

			// Prüfen ob meta.json eines Blocks
			m := reMeta.FindStringSubmatch(key)
			if m != nil {
				prefix := m[1]            // z.B. "anonymous/01ABC..."
				blockID := m[2]           // reiner Block
				prefix = strings.TrimSpace(prefix)

				if !strings.HasSuffix(prefix, "/") {
					prefix += "/"
				}

				fmt.Printf(">>> Erkannter Block: ID=%s PREFIX=%s\n", blockID, prefix)
				foundBlocks[blockID] = prefix
			}
		}

		if !out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}

	fmt.Printf("=== Gefundene Blöcke: %d ===\n", len(foundBlocks))

	if len(foundBlocks) == 0 {
		fmt.Println("KEINE BLÖCKE GEFUNDEN – jetzt ist klar warum dein alter Code nichts fand.")
		return
	}

	// Jetzt pro Block index-header suchen/verschieben
	for blockID, prefix := range foundBlocks {
		indexKey := prefix + "index-header"
		oldKey := prefix + "index-header.old"

		fmt.Printf("Prüfe Block %s → %s\n", blockID, indexKey)

		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(indexKey),
		})

		if err != nil {
			fmt.Printf("Kein index-header in %s – OK\n", prefix)
			continue
		}

		fmt.Printf("Verschiebe %s → %s\n", indexKey, oldKey)

		// Copy
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(oldKey),
			CopySource: aws.String(bucket + "/" + indexKey),
		})
		if err != nil {
			fmt.Printf("ERROR Copy: %v\n", err)
			continue
		}

		time.Sleep(200 * time.Millisecond)

		// Delete
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(indexKey),
		})
		if err != nil {
			fmt.Printf("ERROR Delete: %v\n", err)
			continue
		}

		fmt.Printf("OK: Block %s markiert\n", blockID)
	}

	fmt.Println("=== Fertig ===")
}

