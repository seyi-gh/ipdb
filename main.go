package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	MaxRequests = 100
	WindowSize  = 1 * time.Minute
	BanDuration = 30 * time.Minute
)

type Visitor struct {
	Requests    int
	LastSeen    time.Time
	BannedUntil time.Time
}

type IPInfo struct {
	Network       string `json:"network"`
	CountryCode   string `json:"country_code"`
	CountryName   string `json:"country_name"`
	ContinentName string `json:"continent_name"`
}

var (
	visitors = make(map[string]*Visitor)
	mu       sync.Mutex
	dbPool   *pgxpool.Pool
)

// ? Middleware for Access Control and Rate limit
func SecurityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		now := time.Now()

		mu.Lock()
		v, exists := visitors[ip]

		if !exists {
			visitors[ip] = &Visitor{Requests: 1, LastSeen: now}
			mu.Unlock()
			c.Next()
			return
		}

		//? If the IP is banned block automatically
		if now.Before(v.BannedUntil) {
			timeLeft := time.Until(v.BannedUntil).Round(time.Second)
			mu.Unlock()
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"status":     "banned",
				"message":    "Your IP is in cooldown",
				"expires_in": timeLeft.String(),
			})
			return
		}

		//? Reset the counter if the gap time is over
		if now.Sub(v.LastSeen) > WindowSize {
			v.Requests = 0
			v.LastSeen = now
		}

		v.Requests++

		//? Activate the ban system
		if v.Requests > MaxRequests {
			v.BannedUntil = now.Add(BanDuration)
			mu.Unlock()
			fmt.Printf("BAN IS ACTIVE: %s to %s\n", ip, v.BannedUntil.Format("15:04:05"))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "Too much requests. Your IP is banned temporarily",
			})
			return
		}

		mu.Unlock()
		c.Next()
	}
}

func main() {
	//? Configuration of postgres DATABASE
	dsn := "postgres://postgres:admin@127.0.0.1:5545/ipdb-dev"

	//? Try connect
	connectWithRetry(dsn)
	defer dbPool.Close()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	//? Recovery system
	r.Use(gin.Recovery())
	r.Use(SecurityMiddleware())

	r.GET("/ip/:address", lookupIP)

	//? Health check
	r.GET("/status", func(c *gin.Context) {
		err := dbPool.Ping(context.Background())
		status := "online"
		if err != nil {
			status = "db_reconnecting"
		}
		c.JSON(200, gin.H{"service": "ipdb", "status": status})
	})

	fmt.Println("> Server is running in port :8080")
	r.Run(":8080")
}

// ? Connect database always
func connectWithRetry(dsn string) {
	var err error
	for {
		dbPool, err = pgxpool.New(context.Background(), dsn)
		if err == nil {
			if err = dbPool.Ping(context.Background()); err == nil {
				fmt.Println("> Connection with the database successfully")
				return
			}
		}
		fmt.Printf("> Bad connection with database: %v. Trying again in 5s...\n", err)
		time.Sleep(5 * time.Second)
	}
}

func lookupIP(c *gin.Context) {
	ipAddress := c.Param("address")

	if net.ParseIP(ipAddress) == nil {
		c.JSON(400, gin.H{"error": "IP no valid"})
		return
	}

	//? Short timeout for the API not bug if the db is slow
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	query := `SELECT network::text, country_code, country_name, continent_name 
            FROM ip_blocks WHERE $1::inet <<= network LIMIT 1`

	var info IPInfo
	err := dbPool.QueryRow(ctx, query, ipAddress).Scan(
		&info.Network, &info.CountryCode, &info.CountryName, &info.ContinentName,
	)

	if err != nil {
		if err.Error() == "no rows in result set" {
			c.JSON(404, gin.H{"error": "No data for this IP"})
		} else {
			//? If the database throws an error the server still running
			c.JSON(503, gin.H{"error": "The server is shutdown temporarily"})
		}
		return
	}

	c.JSON(200, info)
}
