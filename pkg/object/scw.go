//go:build !nos3
// +build !nos3

/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package object

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type scw struct {
	s3client
}

func (s *scw) String() string {
	return fmt.Sprintf("scw://%s/", s.s3client.bucket)
}

func (s *scw) Limits() Limits {
	return Limits{
		IsSupportMultipartUpload: true,
		IsSupportUploadPartCopy:  true,
		MinPartSize:              5 << 20,
		MaxPartSize:              5 << 30,
		MaxPartCount:             1000,
	}
}

func newScw(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = fmt.Sprintf("https://%s", endpoint)
	}
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return nil, fmt.Errorf("Invalid endpoint %s: %s", endpoint, err)
	}
	ssl := strings.ToLower(uri.Scheme) == "https"
	hostParts := strings.Split(uri.Host, ".")
	bucket := hostParts[0]
	region := hostParts[2]
	endpoint = uri.Host[len(bucket)+1:]

	if accessKey == "" {
		accessKey = os.Getenv("SCW_ACCESS_KEY")
	}
	if secretKey == "" {
		secretKey = os.Getenv("SCW_SECRET_KEY")
	}

	var options = []func(*s3.Options){disableSha256}
	if !ssl {
		options = append(options, disableHttps)
	}

	client, err := newS3Client(region, bucket, "", endpoint, false, accessKey, secretKey, token, options...)
	return &scw{client}, err
}

func init() {
	Register("scw", newScw)
}
