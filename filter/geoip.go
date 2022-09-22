package filter

import (
	"log"
	"net"

	"sync"

	"github.com/oschwald/geoip2-golang"

	"github.com/childe/gohangout/topology"
)

var Geoip2Reader *geoip2.Reader
var once sync.Once

type GeoIPFilter struct {
	config   map[interface{}]interface{}
	target   string
	database string
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

	return plugin
}

func (plugin *GeoIPFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	ip, ok := plugin.config[plugin.target].(string)
	if !ok {
		return event, true
	}
	ipAddr := net.ParseIP(ip)
	isp, err := GetGeoIPLib(plugin.database).ISP(ipAddr)
	if err != nil {
		return event, true
	}

	record, err := GetGeoIPLib(plugin.database).City(ipAddr)
	if err != nil {
		return event, true
	}

	event["country_name"] = record.Country.Names["en"]
	event["country_code"] = record.Country.IsoCode
	if len(record.Subdivisions) > 0 {
		event["region_name"] = record.Subdivisions[0].Names["en"]
	} else {
		event["region_name"] = ""
	}
	event["city_name"] = record.City.Names["en"]



	event["location"] = map[string]interface{}{
		"lat": record.Location.Latitude,
		"lon": record.Location.Longitude,
	}
	event["isp"] = isp.ISP
	event["organization"] = isp.Organization




	return event, true
}
