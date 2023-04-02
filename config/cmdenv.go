package config

import (
	"fmt"
	"os"
	"reflect"

	"github.com/jessevdk/go-flags"
)

// CmdEnv is a struct that contains all the command line options; it's
// separate from the config struct so that we can apply the command line options
// and env vars after loading the config, and so they don't have to be tied to
// the config struct. Command line options override env vars, and both of them
// override values already in the struct when ApplyCmdEnvTags is called.
// Default values specified in this struct are shown in the help output, but
// most default values should be specified in the config so that the defaults
// system works.
// Note that this system uses reflection to establish the relationship between
// the config struct and the command line options.
type CmdEnv struct {
	ConfigLocation string `long:"config" env:"CONFIG" default:"config.yaml" description:"config file or URL to load"`
	ListenAddr     string `long:"listen-addr" env:"LISTEN_ADDR"`
	PeerListenAddr string `long:"peer-listen-addr" env:"PEER_LISTEN_ADDR"`
}

func NewCmdEnvOptions() (*CmdEnv, error) {
	opts := &CmdEnv{}

	if _, err := flags.Parse(opts); err != nil {
		switch flagsErr := err.(type) {
		case *flags.Error:
			if flagsErr.Type == flags.ErrHelp {
				os.Exit(0)
			}
			return nil, err
		default:
			return nil, err
		}
	}

	return opts, nil
}

// GetField returns the reflect.Value for the field with the given name in the CmdEnvOptions struct.
func (c *CmdEnv) GetField(name string) reflect.Value {
	return reflect.ValueOf(c).Elem().FieldByName(name)
}

// ApplyTags uses reflection to apply the values from the CmdEnv struct to the given struct.
// Any field in the struct that wants to be set from the command line must have a `cmdenv` tag on it that names
// the field in the CmdEnv struct that should be used to set the value. The types must match. If the
// if the name field in CmdEnv field is the zero value, then it will not be applied.
func (c *CmdEnv) ApplyTags(s reflect.Value) error {
	return applyCmdEnvTags(s, c)
}

type getFielder interface {
	GetField(name string) reflect.Value
}

// applyCmdEnvTags is a helper function that applies the values from the given GetFielder to the given struct.
// We do it this way to make it easier to test.
func applyCmdEnvTags(s reflect.Value, fielder getFielder) error {
	switch s.Kind() {
	case reflect.Struct:
		t := s.Type()

		for i := 0; i < s.NumField(); i++ {
			field := s.Field(i)
			fieldType := t.Field(i)

			if tag := fieldType.Tag.Get("cmdenv"); tag != "" {
				// this field has a cmdenv tag, so apply the value from opts
				value := fielder.GetField(tag)
				if !value.IsValid() {
					// if you get this error, you didn't specify cmdenv tags
					// correctly -- its value must be the name of a field in the struct
					return fmt.Errorf("programming error -- invalid field name: %s", tag)
				}
				if !field.CanSet() {
					return fmt.Errorf("programming error -- cannot set new value for: %s", fieldType.Name)
				}

				// don't overwrite values that are already set
				if !value.IsZero() {
					// ensure that the types match
					if fieldType.Type != value.Type() {
						return fmt.Errorf("programming error -- types don't match for field: %s (%v and %v)",
							fieldType.Name, fieldType.Type, value.Type())
					}
					// now we can set it
					field.Set(value)
				}
			}

			// recurse into any nested structs
			err := applyCmdEnvTags(field, fielder)
			if err != nil {
				return err
			}
		}

	case reflect.Ptr:
		if !s.IsNil() {
			return applyCmdEnvTags(s.Elem(), fielder)
		}
	}
	return nil
}
