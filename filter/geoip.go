package filter

import (
	"log"
	"net"

	"sync"

	"github.com/golang/glog"
	"github.com/oschwald/geoip2-golang"

	"github.com/childe/gohangout/topology"
)

var Geoip2Reader *geoip2.Reader
var Geoip2Reader1 *geoip2.Reader
var once sync.Once
var once1 sync.Once

type GeoIPFilter struct {
	config   map[interface{}]interface{}
	target   string
	database string
	ispDatabase string
}

func GetGeoIPLib(database string) *geoip2.Reader {
	var err error

	once.Do(func() {
		Geoip2Reader, err = geoip2.Open(database)
		if err != nil {
			log.Fatal(err.Error())
		}
	})
	return Geoip2Reader
}

func GetGeoIPLibIsp(database string) *geoip2.Reader {
	var err error

	once1.Do(func() {
		Geoip2Reader1, err = geoip2.Open(database)
		if err != nil {
			log.Fatal(err.Error())
		}
	})
	return Geoip2Reader1
}

func init() {
	Register("GeoIP", newGeoIPFilter)
}

func newGeoIPFilter(config map[interface{}]interface{}) topology.Filter {
	plugin := &GeoIPFilter{
		config: config,
		target: "geoip",
		database: "./GeoIP2-City.mmdb",
	}
	if database, ok := config["database"]; ok {
		plugin.database = database.(string)
	}
	if ispDatabase, ok := config["isp_database"]; ok {
		plugin.ispDatabase = ispDatabase.(string)
	}

	return plugin
}

func (plugin *GeoIPFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {

	ipname, ok := plugin.config[plugin.target].(string)
	glog.V(5).Infof("GeoIP Filter %s %+v %+v", ipname, plugin.config, event)
	if !ok {
		glog.Errorf("get ipname %s", plugin.target)
		return event, true
	}
	ip, ok := event[ipname].(string)
	if !ok {
		glog.Errorf("get ip %+v", event)
		return event, true
	}
	glog.V(5).Infof("GeoIP Filter_ip %s %+v %s", ipname, plugin.config, ip)
	ipAddr := net.ParseIP(ip)
	isp, err := GetGeoIPLibIsp(plugin.ispDatabase).ISP(ipAddr)
	if err != nil {
		glog.V(5).Infof("GeoIP Filter_ip err %s %s %+v %s", err, ipname, plugin.config, ip)
		return event, true
	}

	gg := GetGeoIPLib(plugin.database)

	record, err := gg.City(ipAddr)
	if err != nil {
		return event, true
	}

	geoipMap := make(map[string]interface{})
	if len(record.Subdivisions) > 0 {
		geoipMap["region_name"] = record.Subdivisions[0].Names["en"]
	} else {
		geoipMap["region_name"] = ""
	}
	geoipMap["city_name"] = record.City.Names["en"]



	geoipMap["location"] = map[string]interface{}{
		"lat": record.Location.Latitude,
		"lon": record.Location.Longitude,
	}
	geoipMap["isp"] = isp.ISP
	geoipMap["organization"] = isp.Organization
	geoipMap["country_name"] = record.Country.Names["en"]
	geoipMap["country_code"] = record.Country.IsoCode
	event["geoip"] = geoipMap




	return event, true
}
