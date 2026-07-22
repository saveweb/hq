// Package httpapi contains small HTTP/JSON primitives shared by tracker and
// HTTP adapters. It contains no application routing or authorization logic.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/saveweb/hq/pkg/protocol"
)

const ContentTypeJSON = "application/json"

func DecodeJSON(response http.ResponseWriter, request *http.Request, maxBytes int64, destination any) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != ContentTypeJSON {
		return fmt.Errorf("content type must be application/json")
	}
	request.Body = http.MaxBytesReader(response, request.Body, maxBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return fmt.Errorf("request body exceeds %d bytes", maxBytes)
		}
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("request body must contain exactly one JSON object")
	}
	return nil
}

func WriteJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", ContentTypeJSON)
	response.WriteHeader(status)
	encoder := json.NewEncoder(response)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func WriteError(response http.ResponseWriter, status int, apiError protocol.APIError) {
	if apiError.Details == nil {
		apiError.Details = protocol.Attrs{}
	}
	WriteJSON(response, status, protocol.ErrorEnvelope{Error: apiError})
}

func BearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(strings.TrimSpace(header), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", false
	}
	return token, true
}

func SetNoStore(headers http.Header) {
	headers.Set("Cache-Control", "no-store, no-cache, max-age=0")
	headers.Set("Pragma", "no-cache")
	headers.Set("Cloudflare-CDN-Cache-Control", "no-store")
	headers.Set("CDN-Cache-Control", "no-store")
	headers.Set("Expires", "0")
}
