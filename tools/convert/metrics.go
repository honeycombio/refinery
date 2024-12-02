package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"slices"
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/tools/go/packages"
	"gopkg.in/yaml.v3"
)

type MetricsUsage struct {
	Name        string
	Type        string
	Unit        string
	Description string
}

type MetricsOutput struct {
	Complete  []MetricsUsage
	HasPrefix []MetricsUsage
}

const metricsImportPath = "github.com/honeycombio/refinery/metrics"

var packagesContainsPrefix = []string{"route", "main", "sample", "transmit"}

func GenerateMetricsMetadata() error {
	output, err := os.Create("metricsMeta.yaml")
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer output.Close()

	// Configuration to load Go packages.
	cfg := &packages.Config{
		Mode: packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}

	// Load the package from the current directory.
	pkgs, err := packages.Load(cfg, "github.com/honeycombio/refinery/...")
	if err != nil {
		return fmt.Errorf("error loading packages: %v", err)
	}

	var usages MetricsOutput
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
										var usage MetricsUsage
										for _, elt := range comp.Elts {
											if kvExpr, ok := elt.(*ast.KeyValueExpr); ok {
												field := exprToString(kvExpr.Key, pkg)
												value := exprToString(kvExpr.Value, pkg)

												switch field {
												case "Name":
													usage.Name = value
												case "Type":
													usage.Type = value
												case "Unit":
													usage.Unit = value
												case "Description":
													usage.Description = value

												}
											}
										}

										if usage.Name == "" {
											continue
										}
										if slices.Contains(packagesContainsPrefix, pkg.Name) {
											usages.HasPrefix = append(usages.HasPrefix, usage)
										} else {
											usages.Complete = append(usages.Complete, usage)
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
			return fmt.Errorf("Missing metrics.Metadata declaration in the package %s", pkg.Name)
		}
	}

	if len(usages.Complete) == 0 && len(usages.HasPrefix) == 0 {
		return fmt.Errorf("No metrics.Metadata declarations found in all packages")
	}

	err = writeMetricsToYAML(usages, output)
	if err != nil {
		return fmt.Errorf("error writing metrics to YAML: %v", err)
	}

	fmt.Printf("Metrics usages have been written to %s\n", output.Name())
	return nil

}

// exprToString is a helper function to convert ast.Expr to a string representation
func exprToString(expr ast.Expr, pkg *packages.Package) string {
	var strVal string
	switch v := expr.(type) {
	case *ast.Ident:
		if obj := pkg.TypesInfo.ObjectOf(v); obj != nil {
			// Get the value of the variable (if constant)
			if constVal, ok := obj.(*types.Const); ok {
				strVal = constVal.Val().String()
				break
			}
		}
		strVal = v.Name
	case *ast.BasicLit:
		strVal = v.Value
	case *ast.SelectorExpr:
		strVal = v.Sel.Name
	default:
		strVal = fmt.Sprintf("%T", expr)
	}

	return strings.Trim(strVal, "\"")
}

func writeMetricsToYAML(metricsUsages MetricsOutput, output *os.File) error {
	// Create a new YAML encoder and write the metrics
	encoder := yaml.NewEncoder(output)
	defer encoder.Close()

	err := encoder.Encode(metricsUsages)
	if err != nil {
		return fmt.Errorf("error encoding YAML: %v", err)
	}

	return nil
}
