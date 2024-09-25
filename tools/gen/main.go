package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/jessevdk/go-flags"
	"golang.org/x/exp/maps"
	"golang.org/x/tools/go/packages"
)

type Options struct {
	Output string `short:"o" long:"output" description:"the metrics documentation to write (goes to stdout by default)"`
}

type MetricsUsage struct {
	Name        string
	Type        string
	Unit        string
	Description string
}

const metricsImportPath = "github.com/honeycombio/refinery/metrics"

func main() {
	opts := Options{}

	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		switch flagsErr := err.(type) {
		case *flags.Error:
			if flagsErr.Type == flags.ErrHelp {
				os.Exit(0)
			}
			os.Exit(1)
		default:
			os.Exit(1)
		}
	}

	output := os.Stdout
	if opts.Output != "" {
		output, err = os.Create(opts.Output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "'%v' opening %s for writing\n", err, opts.Output)
			os.Exit(1)
		}
		defer output.Close()
	}

	// Configuration to load Go packages.
	cfg := &packages.Config{
		Mode: packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}

	// Load the package from the current directory.
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		log.Fatal(err)
	}

	// Traverse each package and file.
	for _, pkg := range pkgs {
		if !slices.Contains(maps.Keys(pkg.Imports), metricsImportPath) {
			continue
		}

		var found bool
		// Iterate over the syntax trees (ASTs) of each file in the package.
		for _, syntax := range pkg.Syntax {
			// Inspect the AST for each file.
			ast.Inspect(syntax, func(n ast.Node) bool {
				// Look for all slice type declarations
				if decl, ok := n.(*ast.CompositeLit); ok {

					if arrayType, ok := decl.Type.(*ast.ArrayType); ok {
						// Check if the element type of the array is a selector expression
						if selector, ok := arrayType.Elt.(*ast.SelectorExpr); ok {
							// Check if the package and type name match "metrics.Metadata"
							if pkgIdent, ok := selector.X.(*ast.Ident); ok && pkgIdent.Name == "metrics" && selector.Sel.Name == "Metadata" {

								// Now extract the fields from the composite literal
								for _, elt := range decl.Elts {
									if comp, ok := elt.(*ast.CompositeLit); ok {
										var sb strings.Builder
										for _, elt := range comp.Elts {
											if kvExpr, ok := elt.(*ast.KeyValueExpr); ok {
												field := exprToString(kvExpr.Key, pkg)
												value := exprToString(kvExpr.Value, pkg)

												sb.WriteString(fmt.Sprintf("%s: %s\n", field, value))
											}
										}

										// Add an empty line after each block for readability
										sb.WriteString("\n")

										// Write the markdown content to the file
										_, err := output.WriteString(sb.String())
										if err != nil {
											log.Fatalf("Failed to write to markdown file: %v", err)
										}

										found = true
									}
								}
							}
						}
					}
				}
				return true
			})

		}
		if !found {
			log.Fatalf("Missing metrics.Metadata declaration in the package %s", pkg.Name)
		}
	}

	fmt.Printf("Metrics usages have been written to %s\n", output.Name())
}

// exprToString is a helper function to convert ast.Expr to a string representation
func exprToString(expr ast.Expr, pkg *packages.Package) string {
	switch v := expr.(type) {
	case *ast.Ident:
		if obj := pkg.TypesInfo.ObjectOf(v); obj != nil {
			// Get the value of the variable (if constant)
			if constVal, ok := obj.(*types.Const); ok {
				return strings.Trim(constVal.Val().String(), "\"")
			}

		}
		return v.Name
	case *ast.BasicLit:
		return v.Value
	case *ast.SelectorExpr:
		return exprToString(v.X, pkg) + "." + v.Sel.Name
	default:
		return fmt.Sprintf("%T", expr)
	}
}
