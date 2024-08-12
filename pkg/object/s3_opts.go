package object

import (
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
)

func disableHttps(options *s3.Options) {
	options.EndpointOptions.DisableHTTPS = true
}

func disable100s(options *s3.Options) {
	options.ContinueHeaderThresholdBytes = -1
}

func usePathStyle(options *s3.Options) {
	options.UsePathStyle = true
}

func disableSha256(options *s3.Options) {
	options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
		if stack.ID() == "PutObject" || stack.ID() == "UploadPart" {
			err := v4.RemoveContentSHA256HeaderMiddleware(stack)
			if err != nil {
				return err
			}

			err = v4.RemoveComputePayloadSHA256Middleware(stack)
			if err != nil {
				return err
			}

			return v4.AddUnsignedPayloadMiddleware(stack)
		}
		return nil
	})
}
