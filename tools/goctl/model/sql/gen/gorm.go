package gen

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/tal-tech/go-zero/tools/goctl/config"
	"github.com/tal-tech/go-zero/tools/goctl/model/sql/model"
	"github.com/tal-tech/go-zero/tools/goctl/model/sql/parser"
	"github.com/tal-tech/go-zero/tools/goctl/model/sql/template"
	modelutil "github.com/tal-tech/go-zero/tools/goctl/model/sql/util"
	"github.com/tal-tech/go-zero/tools/goctl/util"
	"github.com/tal-tech/go-zero/tools/goctl/util/console"
	"github.com/tal-tech/go-zero/tools/goctl/util/format"
	"github.com/tal-tech/go-zero/tools/goctl/util/stringx"
)

type (
	gormGenerator struct {
		// source string
		dir string
		console.Console
		pkg string
		cfg *config.Config
	}

	// gormOption defines a function with argument gormGenerator
	gormOption func(generator *gormGenerator)

	gormCode struct {
		importsCode string
		varsCode    string
		typesCode   string
		newCode     string
		insertCode  string
		findCode    []string
		updateCode  string
		deleteCode  string
		cacheExtra  string
	}
)

// NewgormGenerator creates an instance for gormGenerator
func NewgormGenerator(dir string, cfg *config.Config, opt ...gormOption) (*gormGenerator, error) {
	if dir == "" {
		dir = pwd
	}
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	dir = dirAbs
	pkg := filepath.Base(dirAbs)
	err = util.MkdirIfNotExist(dir)
	if err != nil {
		return nil, err
	}

	generator := &gormGenerator{dir: dir, cfg: cfg, pkg: pkg}
	var optionList []gormOption
	optionList = append(optionList, newDefaultgormOption())
	optionList = append(optionList, opt...)
	for _, fn := range optionList {
		fn(generator)
	}

	return generator, nil
}

func newDefaultgormOption() gormOption {
	return func(generator *gormGenerator) {
		generator.Console = console.NewColorConsole()
	}
}

func (g *gormGenerator) StartFromDDL(source string, withCache bool) error {
	modelList, err := g.genFromDDL(source, withCache)
	if err != nil {
		return err
	}

	return g.createFile(modelList)
}

func (g *gormGenerator) StartFromInformationSchema(tables map[string]*model.Table, withCache bool) error {
	m := make(map[string]string)
	for _, each := range tables {
		table, err := parser.ConvertDataType(each)
		if err != nil {
			return err
		}

		code, err := g.genModel(*table, withCache)
		if err != nil {
			return err
		}

		m[table.Name.Source()] = code
	}

	return g.createFile(m)
}

func (g *gormGenerator) createFile(modelList map[string]string) error {
	dirAbs, err := filepath.Abs(g.dir)
	if err != nil {
		return err
	}

	g.dir = dirAbs
	g.pkg = filepath.Base(dirAbs)
	err = util.MkdirIfNotExist(dirAbs)
	if err != nil {
		return err
	}

	for tableName, code := range modelList {
		tn := stringx.From(tableName)
		modelFilename, err := format.FileNamingFormat(g.cfg.NamingFormat, fmt.Sprintf("%s_model", tn.Source()))
		if err != nil {
			return err
		}

		name := modelFilename + ".go"
		filename := filepath.Join(dirAbs, name)
		if util.FileExists(filename) {
			g.Warning("%s already exists, ignored.", name)
			continue
		}
		err = ioutil.WriteFile(filename, []byte(code), os.ModePerm)
		if err != nil {
			return err
		}
	}

	// generate error file
	varFilename, err := format.FileNamingFormat(g.cfg.NamingFormat, "vars")
	if err != nil {
		return err
	}

	filename := filepath.Join(dirAbs, varFilename+".go")
	text, err := util.LoadTemplate(category, errTemplateFile, template.Error)
	if err != nil {
		return err
	}

	err = util.With("vars").Parse(text).SaveTo(map[string]interface{}{
		"pkg": g.pkg,
	}, filename, false)
	if err != nil {
		return err
	}

	g.Success("Done.")
	return nil
}

// ret1: key-table name,value-code
func (g *gormGenerator) genFromDDL(source string, withCache bool) (map[string]string, error) {
	ddlList := g.split(source)
	m := make(map[string]string)
	for _, ddl := range ddlList {
		table, err := parser.Parse(ddl)
		if err != nil {
			return nil, err
		}

		code, err := g.genModel(*table, withCache)
		if err != nil {
			return nil, err
		}

		m[table.Name.Source()] = code
	}

	return m, nil
}

func (g *gormGenerator) split(source string) []string {
	reg := regexp.MustCompile(createTableFlag)
	index := reg.FindAllStringIndex(source, -1)
	list := make([]string, 0)

	for i := len(index) - 1; i >= 0; i-- {
		subIndex := index[i]
		if len(subIndex) == 0 {
			continue
		}
		start := subIndex[0]
		ddl := source[start:]
		list = append(list, ddl)
		source = source[:start]
	}

	return list
}

func (g *gormGenerator) genModel(in parser.Table, withCache bool) (string, error) {
	if len(in.PrimaryKey.Name.Source()) == 0 {
		return "", fmt.Errorf("table %s: missing primary key", in.Name.Source())
	}

	primaryKey, uniqueKey := genCacheKeys(in)

	importsCode, err := genImports(withCache, in.ContainsTime())
	if err != nil {
		return "", err
	}

	var table Table
	table.Table = in
	table.PrimaryCacheKey = primaryKey
	table.UniqueCacheKey = uniqueKey
	table.ContainsUniqueCacheKey = len(uniqueKey) > 0

	varsCode, err := genVars(table, withCache)
	if err != nil {
		return "", err
	}

	insertCode, insertCodeMethod, err := genInsert(table, withCache)
	if err != nil {
		return "", err
	}

	findCode := make([]string, 0)
	findOneCode, findOneCodeMethod, err := genFindOne(table, withCache)
	if err != nil {
		return "", err
	}

	ret, err := genFindOneByField(table, withCache)
	if err != nil {
		return "", err
	}

	findCode = append(findCode, findOneCode, ret.findOneMethod)
	updateCode, updateCodeMethod, err := genUpdate(table, withCache)
	if err != nil {
		return "", err
	}

	deleteCode, deleteCodeMethod, err := genDelete(table, withCache)
	if err != nil {
		return "", err
	}

	var list []string
	list = append(list, insertCodeMethod, findOneCodeMethod, ret.findOneInterfaceMethod, updateCodeMethod, deleteCodeMethod)
	typesCode, err := genTypes(table, strings.Join(modelutil.TrimStringSlice(list), util.NL), withCache)
	if err != nil {
		return "", err
	}

	newCode, err := genNew(table, withCache)
	if err != nil {
		return "", err
	}

	code := &gormCode{
		importsCode: importsCode,
		varsCode:    varsCode,
		typesCode:   typesCode,
		newCode:     newCode,
		insertCode:  insertCode,
		findCode:    findCode,
		updateCode:  updateCode,
		deleteCode:  deleteCode,
		cacheExtra:  ret.cacheExtra,
	}

	output, err := g.executeModel(code)
	if err != nil {
		return "", err
	}

	return output.String(), nil
}

func (g *gormGenerator) executeModel(code *gormCode) (*bytes.Buffer, error) {
	text, err := util.LoadTemplate(category, modelTemplateFile, template.Model)
	if err != nil {
		return nil, err
	}
	t := util.With("model").
		Parse(text).
		GoFmt(true)
	output, err := t.Execute(map[string]interface{}{
		"pkg":         g.pkg,
		"imports":     code.importsCode,
		"vars":        code.varsCode,
		"types":       code.typesCode,
		"new":         code.newCode,
		"insert":      code.insertCode,
		"find":        strings.Join(code.findCode, "\n"),
		"update":      code.updateCode,
		"delete":      code.deleteCode,
		"extraMethod": code.cacheExtra,
	})
	if err != nil {
		return nil, err
	}
	return output, nil
}
