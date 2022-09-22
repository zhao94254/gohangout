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

func GetGeoIPLib() *geoip2.Reader {
	var err error

	once.Do(func() {
		Geoip2Reader, err = geoip2.Open("./GeoIP2-City.mmdb")
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
	}

	return plugin
}

func (plugin *GeoIPFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	ip, ok := plugin.config[plugin.target].(string)
	if !ok {
		return event, true
	}
	ipAddr := net.ParseIP(ip)
	record, err := GetGeoIPLib().City(ipAddr)
	if err != nil {
		return nil, true
	}

	event["country"] = record.Country.Names["en"]
	event["country_code"] = record.Country.IsoCode
	if len(record.Subdivisions) > 0 {
		event["region"] = record.Subdivisions[0].Names["en"]
	} else {
		event["region"] = ""
	}
	event["city"] = record.City.Names["en"]
	event["lat"] = record.Location.Latitude
	event["lon"] = record.Location.Longitude

	return event, true
}
