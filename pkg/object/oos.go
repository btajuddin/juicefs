//go:build !nos3
// +build !nos3

/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
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
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type oos struct {
	s3client
}

func (s *oos) String() string {
	return fmt.Sprintf("oos://%s/", s.s3client.bucket)
}

func (s *oos) Limits() Limits {
	return Limits{
		IsSupportMultipartUpload: true,
	}
}

func (s *oos) Create() error {
	_, err := s.List("", "", "", 1, true)
	if err != nil {
		return fmt.Errorf("please create bucket %s manually", s.s3client.bucket)
	}
	return err
}

func (s *oos) List(prefix, marker, delimiter string, limit int64, followLink bool) ([]Object, error) {
	if limit > 1000 {
		limit = 1000
	}
	objs, err := s.s3client.List(prefix, marker, delimiter, limit, followLink)
	if marker != "" && len(objs) > 0 && objs[0].Key() == marker {
		objs = objs[1:]
	}
	return objs, err
}

func newOOS(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
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
	region := hostParts[1][4:]
	endpoint = uri.Host[len(bucket)+1:]
	forcePathStyle := !strings.Contains(strings.ToLower(endpoint), "xstore.ctyun.cn")

	var options = []func(*s3.Options){disableSha256}
	if forcePathStyle {
		options = append(options, usePathStyle)
	}
	if !ssl {
		options = append(options, disableHttps)
	}

	client, err := newS3Client(region, bucket, "", endpoint, false, accessKey, secretKey, token, options...)
	return &oos{client}, err
}

func init() {
	Register("oos", newOOS)
}
