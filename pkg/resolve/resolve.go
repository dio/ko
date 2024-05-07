// Copyright 2018 ko Build Authors All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resolve

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/dio/ko/pkg/build"
	"github.com/dio/ko/pkg/publish"
	"github.com/dprotaso/go-yit"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

type nodeRef struct {
	part string
	node *yaml.Node
}

// ImageReferences resolves supported references to images within the input yaml
// to published image digests.
//
// If a reference can be built and pushed, its yaml.Node will be mutated.
func ImageReferences(ctx context.Context, docs []*yaml.Node, builder build.Interface, publisher publish.Interface) error {
	// First, walk the input objects and collect a list of supported references
	refs := make(map[string][]*nodeRef)

	for _, doc := range docs {
		it := refsFromDoc(doc)

		for node, ok := it(); ok; node, ok = it() {
			ref := strings.TrimSpace(node.Value)

			parsed, err := url.Parse(ref)
			if err != nil {
				return fmt.Errorf("failed to parse %q: %w", ref, err)
			}
			ref = parsed.Scheme + "://" + parsed.Host + parsed.Path

			if err := builder.IsSupportedReference(ref); err != nil {
				return fmt.Errorf("found strict reference but %s is not a valid import path: %w", ref, err)
			}

			parsedQuery, err := url.ParseQuery(parsed.RawQuery)
			if err != nil {
				return fmt.Errorf("failed to parse query %q: %w", parsed.RawQuery, err)
			}

			if len(parsedQuery) == 0 {
				refs[ref] = append(refs[ref], &nodeRef{part: "all", node: node})
				continue
			}
			refs[ref] = append(refs[ref], &nodeRef{part: parsedQuery["part"][0], node: node})
		}
	}

	// Next, perform parallel builds for each of the supported references.
	var sm sync.Map
	var errg errgroup.Group
	for ref := range refs {
		ref := ref
		errg.Go(func() error {
			img, err := builder.Build(ctx, ref)
			if err != nil {
				return err
			}
			digest, err := publisher.Publish(ctx, img, ref)
			if err != nil {
				return err
			}
			sm.Store(ref, digest.String())
			return nil
		})
	}
	if err := errg.Wait(); err != nil {
		return err
	}

	// Walk the tags and update them with their digest.
	for ref, nodes := range refs {
		digest, ok := sm.Load(ref)

		if !ok {
			return fmt.Errorf("resolved reference to %q not found", ref)
		}

		fmt.Fprintln(os.Stderr, "ref: ", ref)

		for _, node := range nodes {
			d := digest.(string)
			parsed, err := url.Parse(d)
			if err != nil {
				return fmt.Errorf("failed to parse %q: %w", d, err)
			}

			switch node.part {
			case "registry":
				dir := path.Dir(parsed.Path)
				node.node.Value = fmt.Sprintf("%s%s", parsed.Host, dir)
			case "repository":
				if strings.Contains(parsed.Path, ":") {
					node.node.Value = fmt.Sprintf("%s%s", parsed.Host, parsed.Path[:strings.Index(parsed.Path, ":")])
				} else {
					node.node.Value = fmt.Sprintf("%s%s", parsed.Host, parsed.Path[:strings.Index(parsed.Path, "@")])
				}
			case "name":
				basePath := path.Base(parsed.Path)
				node.node.Value = basePath
				if strings.Contains(basePath, ":") {
					node.node.Value = basePath[:strings.Index(basePath, ":")]
				} else if strings.Contains(basePath, "@") {
					node.node.Value = basePath[:strings.Index(basePath, "@")]
				}
			case "tag":
				if strings.Contains(parsed.Path, "@") {
					node.node.Value = "latest@" + parsed.Path[strings.Index(parsed.Path, "@")+1:]
				} else {
					node.node.Value = parsed.Path[strings.Index(parsed.Path, ":")+1:]
				}
			case "tagWithSeparator":
				if strings.Contains(parsed.Path, "@") {
					node.node.Value = parsed.Path[strings.Index(parsed.Path, "@"):]
				} else {
					node.node.Value = parsed.Path[strings.Index(parsed.Path, ":"):]
				}
			default:
				if strings.HasPrefix(node.part, "definedRegistry=") {
					fmt.Fprintln(os.Stderr, "WARNING: definedRegistry is set to", node.part)
					parts := strings.SplitN(node.part, "=", 2)
					if len(parts) != 2 {
						return fmt.Errorf("invalid definedRegistry part: %s", node.part)
					}

					node.node.Value = parts[1]
					fmt.Fprintln(os.Stderr, "node.node.Value", node.node.Value)
				} else {
					node.node.Value = d
				}
			}
		}
	}

	return nil
}

func refsFromDoc(doc *yaml.Node) yit.Iterator {
	it := yit.FromNode(doc).
		RecurseNodes().
		Filter(yit.StringValue)

	return it.Filter(yit.WithPrefix(build.StrictScheme))
}
