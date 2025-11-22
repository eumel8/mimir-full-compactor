package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	fmt.Println("=== Starte Verbindung zu S3 ===")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.Fatalf("Fehler beim Laden der Config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFromURL(endpoint)
		o.UsePathStyle = true
	})

	ctx := context.Background()

	// Regex für Mimir Block-IDs: 12+ alphanumerische Zeichen
	blockRegex := regexp.MustCompile(`^[0-9A-Za-z]{12,}/$`)

	fmt.Println("=== Suche rekursiv nach Blöcken ===")
	blocks := findBlocks(ctx, client, bucket, "", blockRegex)

	fmt.Printf("Gefundene echte Blöcke: %d\n", len(blocks))
	if len(blocks) == 0 {
		fmt.Println("Keine Blöcke zum Verarbeiten gefunden. Script beendet.")
		return
	}

	// Parallelisierung
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)

	for _, block := range blocks {
		wg.Add(1)
		go func(block string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			indexHeaderKey := block + "index-header"
			newKey := block + "index-header.old"

			fmt.Printf("Prüfe index-header für Block %s\n", block)
			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(indexHeaderKey),
			})
			if err != nil {
				fmt.Printf("Block %s hat noch keinen index-header, wird trotzdem verarbeitet\n", block)
			} else {
				fmt.Printf("Block %s hat index-header, verschiebe nach .old\n", block)

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

				// Copy
				err = retry(func() error {
					fmt.Printf("CopyObject: %s -> %s\n", indexHeaderKey, newKey)
					_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
						Bucket:     aws.String(bucket),
						CopySource: aws.String(bucket + "/" + indexHeaderKey),
						Key:        aws.String(newKey),
					})
					return err
				})
				if err != nil {
					fmt.Printf("WARN: Konnte %s nicht kopieren: %v\n", indexHeaderKey, err)
				} else {
					// Delete
					_ = retry(func() error {
						fmt.Printf("DeleteObject: %s\n", indexHeaderKey)
						_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(bucket),
							Key:    aws.String(indexHeaderKey),
						})
						return err
					})
				}
			}

			fmt.Printf("Block %s für Compactor markiert\n", block)
		}(block)
	}

	wg.Wait()
	fmt.Println("Fertig! Jetzt den Compactor starten, um neue Sparse Index-Header zu erzeugen.")
}

// findBlocks durchsucht rekursiv alle Unterordner nach Block-Ordnern
func findBlocks(ctx context.Context, client *s3.Client, bucket string, prefix string, blockRegex *regexp.Regexp) []string {
	var blocks []string
	var nextPrefixes []string

	maxKeys := int32(1000)
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
		MaxKeys:   &maxKeys,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("Fehler beim Listen von S3 unter Prefix '%s': %v", prefix, err)
		}

		for _, cp := range page.CommonPrefixes {
			dir := *cp.Prefix
			if blockRegex.MatchString(dir) {
				fmt.Printf("Erkannter Block: %s\n", dir)
				blocks = append(blocks, dir)
			} else {
				fmt.Printf("Rekursiver Scan in Unterordner: %s\n", dir)
				nextPrefixes = append(nextPrefixes, dir)
			}
		}
	}

	for _, p := range nextPrefixes {
		blocks = append(blocks, findBlocks(ctx, client, bucket, p, blockRegex)...)
	}

	return blocks
}

