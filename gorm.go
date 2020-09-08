package emgorm

import (
	"errors"
	"github.com/go-emix/utils"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

type DbSet map[string]*gorm.DB

var _dbs = DbSet{}

var DefaultPageCount int64 = 10

func GetDb(name string) *gorm.DB {
	return _dbs[name]
}

func SetDb(name string, db *gorm.DB) {
	if name == "" {
		panic("name not be empty")
	}
	_dbs[name] = db
}

func SetDbFromOption(name string, op Option) error {
	if name == "" {
		panic("name not be empty")
	}
	connect, err := OpenConnect(op)
	if err != nil {
		return err
	}
	_dbs[name] = connect
	return nil
}

func IsEmpty(set ...DbSet) bool {
	if len(set) != 0 {
		return len(set[0]) == 0
	}
	return len(_dbs) == 0
}

type RootConfig struct {
	Emix EmixConfig `yaml:"emix"`
}

type EmixConfig struct {
	Db []DbConfig `yaml:"db,flow"`
}

type DbConfig struct {
	Name            string `yaml:"name"`
	Dialect         string `yaml:"dialect"`
	Connect         string `yaml:"connect"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	MaxOpenConns    int    `yaml:"maxOpenConns"`
	ConnMaxLifetime int64  `yaml:"connMaxLifetime"`
}

func (r DbConfig) Option() Option {
	return Option{
		Dialect:         r.Dialect,
		Connect:         r.Connect,
		MaxIdleConns:    r.MaxIdleConns,
		MaxOpenConns:    r.MaxOpenConns,
		ConnMaxLifetime: time.Duration(r.ConnMaxLifetime) * time.Second,
	}
}

type Option struct {
	Dialect         string
	Connect         string
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
}

var ConnectBeEmptyErr = errors.New("connect be empty")

func OpenConnect(op Option) (db *gorm.DB, err error) {
	if op.Connect == "" {
		err = ConnectBeEmptyErr
		return
	}
	if op.Dialect == "" {
		op.Dialect = "mysql"
	}
	db, err = gorm.Open(op.Dialect, op.Connect)
	if err != nil {
		return
	}
	if op.MaxIdleConns > 0 {
		db.DB().SetMaxIdleConns(op.MaxIdleConns)
	}
	if op.MaxOpenConns > 0 {
		db.DB().SetMaxOpenConns(op.MaxOpenConns)
	}
	if op.ConnMaxLifetime > 0 {
		db.DB().SetConnMaxLifetime(op.ConnMaxLifetime)
	}
	return
}

type MsqlPager struct {
	db        *gorm.DB
	PageNum   int64
	PageCount int64
	Pages     int64
	Counts    int64
}

func NewMsqlPager(db *gorm.DB, pageNum int64, pageCount int64) *MsqlPager {
	if pageNum < 1 {
		pageNum = 1
	}
	if pageCount <= 0 {
		pageCount = DefaultPageCount
	}
	return &MsqlPager{db: db, PageNum: pageNum, PageCount: pageCount}
}

func (r *MsqlPager) Raw(sql string, values ...interface{}) *MsqlPager {
	r.Counts = 0
	r.Pages = 0
	r.db.Raw("select count(1) counts from ("+sql+") c ",
		values...).Scan(&r)
	if r.Counts == 0 {
		return r
	}
	if r.Counts <= r.PageCount {
		r.Pages = 1
		r.db = r.db.Raw(sql, values...)
	} else {
		i := r.Counts % r.PageCount
		r.Pages = r.Counts / r.PageCount
		if i != 0 {
			r.Pages += 1
		}
		if r.PageNum > r.Pages {
			r.PageNum = r.Pages
		}
		start := (r.PageNum - 1) * r.PageCount
		r.db = r.db.Raw(sql+" limit "+strconv.FormatInt(start, 10)+","+
			strconv.FormatInt(r.PageCount, 10),
			values...)
	}
	return r
}

func (r *MsqlPager) Scan(slice interface{}) error {
	if r.Counts == 0 {
		return gorm.ErrRecordNotFound
	}
	return r.db.Scan(slice).Error
}

func parseYaml(file string) []DbConfig {
	bytes, e := ioutil.ReadFile(file)
	utils.PanicError(e)
	yc := RootConfig{}
	e = yaml.Unmarshal(bytes, &yc)
	utils.PanicError(e)
	return yc.Emix.Db
}

func init() {
	var ymlFile = "config.yml"
	var yamlFile = "config.yaml"
	var cs = make([]DbConfig, 0)
	if !utils.FileIsExist(ymlFile) {
		if utils.FileIsExist(yamlFile) {
			cs = parseYaml(yamlFile)
		}
	} else {
		cs = parseYaml(ymlFile)
	}
	for _, v := range cs {
		err := SetDbFromOption(v.Name, v.Option())
		utils.PanicError(err)
	}
}

type Operation struct {
	dcs []DbConfig
}

func (r Operation) Setup() {
	if len(r.dcs) != 0 {
		_dbs = make(map[string]*gorm.DB)
		for _, v := range r.dcs {
			err := SetDbFromOption(v.Name, v.Option())
			utils.PanicError(err)
		}
	}
}

func AfterInit(yamlFile string, cs ...DbConfig) (oper Operation) {
	if len(cs) != 0 {
		oper.dcs = cs
		return
	}
	if yamlFile != "" && utils.FileIsExist(yamlFile) {
		cs := parseYaml(yamlFile)
		oper.dcs = cs
	}
	return
}

func RFC3339() string {
	index := strings.Index(time.RFC3339, "Z")
	return strings.ReplaceAll(time.RFC3339[:index], "T", " ")
}

func Uint8ToTime(ua []uint8) (time.Time, error) {
	rfc := RFC3339()
	index := strings.Index(rfc, " ")
	s := rfc[:index+1] + string(ua)
	return time.Parse(rfc, s)
}
