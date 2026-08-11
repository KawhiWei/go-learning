package openapi

import _ "embed"

var (
	//go:embed user.swagger.yaml
	Spec []byte

	//go:embed swagger.html
	SwaggerUI []byte
)
