package common

type Config struct {
	LogPath      string   `json:"log_path"`
	LogFile      string   `json:"log_file"`
	Symbol       string   `json:"symbol"`
	URL          string   `json:"url"`
	KafkaBroker  string   `json:"kafka_broker"`
	Topic        string   `json:"topic"`
	Topics       []string `json:"topics"`
	Ptopic       string   `json:"publish_topic"`
	TimeInterval int      `json:"time_interval"`
	LocationID   string   `json:"location_id"`
	Currency     string   `json:"currency"`
}
