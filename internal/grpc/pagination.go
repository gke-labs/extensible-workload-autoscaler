package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// PageRequest is a paginated RPC request whose page token can be set.
type PageRequest interface {
	SetPageToken(string)
}

// PageResponse is a paginated RPC response that exposes the next page token.
type PageResponse interface {
	GetNextPageToken() string
}

// ListAllPages automatically handles RPC List request pagination, following
// next_page_token until the last page. If any page fails, it returns nil and
// the error so callers never mistake a partial list for the full one.
// ctx bounds the whole listing across all pages.
func ListAllPages[Req PageRequest, Resp PageResponse, Item any](
	ctx context.Context,
	req Req,
	call func(context.Context, Req, ...grpc.CallOption) (Resp, error),
	extract func(Resp) []Item,
	opts ...grpc.CallOption,
) ([]Item, error) {
	var allItems []Item
	token := ""
	for {
		req.SetPageToken(token)
		resp, err := call(ctx, req, opts...)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, extract(resp)...)
		token = resp.GetNextPageToken()
		if token == "" {
			return allItems, nil
		}
	}
}
