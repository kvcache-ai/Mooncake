package main

import (
	"flag"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

const jsonContentType = "application/json; charset=utf-8"

type MetadataStore struct {
	store sync.Map
}

var (
	metadataStore = MetadataStore{}
)

func (m *MetadataStore) Get(key string) ([]byte, bool) {
	value, ok := m.store.Load(key)
	if !ok {
		return nil, false
	}
	return value.([]byte), true
}

func (m *MetadataStore) Set(key string, value []byte) {
	m.store.Store(key, value)
}

func (m *MetadataStore) Delete(key string) {
	m.store.Delete(key)
}

func getQueryKey(c *gin.Context) string {
	return c.Request.URL.Query().Get("key")
}

func getMetadata(c *gin.Context) {
	key := getQueryKey(c)
	value, ok := metadataStore.Get(key)
	if !ok {
		c.Data(http.StatusNotFound, jsonContentType, []byte(`metadata not found`))
		return
	}
	c.Data(http.StatusOK, jsonContentType, value)
}

func putMetadata(c *gin.Context) {
	key := getQueryKey(c)
	value, err := c.GetRawData()
	if err != nil {
		c.Data(http.StatusBadRequest, jsonContentType, []byte(`invalid request`))
		return
	}
	metadataStore.Set(key, value)
	c.Data(http.StatusOK, jsonContentType, []byte(`metadata updated`))
}

func deleteMetadata(c *gin.Context) {
	key := getQueryKey(c)
	metadataStore.Delete(key)
	c.Data(http.StatusOK, jsonContentType, []byte(`metadata deleted`))
}

func main() {
	address := flag.String("addr", ":8080", "HTTP server address (default :8080)")
	flag.Parse()

	r := gin.Default()

	r.GET("/metadata", getMetadata)
	r.PUT("/metadata", putMetadata)
	r.DELETE("/metadata", deleteMetadata)

	r.Run(*address)
}
