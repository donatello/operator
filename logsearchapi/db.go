package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	createTablePartition QTemplate = `CREATE TABLE %s PARTITION OF %s
                                            FOR VALUES FROM ('%s') TO ('%s');`
)

const (
	partitionsPerMonth = 4
)

// QTemplate is used to represent queries that involve string substitution as
// well as SQL positional argument substitution.
type QTemplate string

func (t QTemplate) build(args ...interface{}) string {
	return fmt.Sprintf(string(t), args...)
}

// Table a database table
type Table struct {
	Name            string
	CreateStatement QTemplate
}

func (t *Table) getCreateStatement() string {
	return t.CreateStatement.build(t.Name)
}

func (t *Table) getCreatePartitionStatement(partitionNameSuffix, rangeStart, rangeEnd string) string {
	partitionName := fmt.Sprintf("%s_%s", t.Name, partitionNameSuffix)
	return createTablePartition.build(partitionName, t.Name, rangeStart, rangeEnd)
}

var (
	auditLogEventsTable = Table{
		Name: "audit_log_events",
		CreateStatement: `CREATE TABLE %s (
                                    event_time TIMESTAMPTZ NOT NULL,
                                    log JSONB NOT NULL
                                  ) PARTITION BY RANGE (event_time);`,
	}
	requestInfoTable = Table{
		Name: "request_info",
		CreateStatement: `CREATE TABLE %s (
                                    time TIMESTAMPTZ NOT NULL,
                                    api_name TEXT NOT NULL,
                                    bucket TEXT,
                                    object TEXT,
                                    time_to_response_ns INT8,
                                    remote_host TEXT,
                                    request_id TEXT,
                                    user_agent TEXT,
                                    response_status TEXT,
                                    response_status_code INT8,
                                    request_content_length INT8,
                                    response_content_length INT8
                                  ) PARTITION BY RANGE (time);`,
	}
)

func getPartitionRange(t time.Time) (time.Time, time.Time) {
	// Zero out the time and use UTC
	t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	daysInMonth := t.AddDate(0, 1, -t.Day()).Day()
	quot := daysInMonth / partitionsPerMonth
	remDays := daysInMonth % partitionsPerMonth
	rangeStart := t.AddDate(0, 0, 1-t.Day())
	for {
		rangeDays := quot
		if remDays > 0 {
			rangeDays++
			remDays--
		}
		rangeEnd := rangeStart.AddDate(0, 0, rangeDays)
		if t.Before(rangeEnd) {
			return rangeStart, rangeEnd
		}
		rangeStart = rangeEnd
	}
}

// DBClient is a client object that makes requests to the DB.
type DBClient struct {
	*pgxpool.Pool
}

// NewDBClient creates a new DBClient.
func NewDBClient(ctx context.Context, connStr string) (*DBClient, error) {
	pool, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}
	return &DBClient{pool}, nil
}

func (c *DBClient) checkTableExists(ctx context.Context, table string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	const existsQuery QTemplate = `SELECT 1 FROM %s WHERE false;`
	res, _ := c.Query(ctx, existsQuery.build(table))
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "(SQLSTATE 42P01)") {
			return false, nil
		}
		return false, res.Err()
	}
	return true, nil
}

func (c *DBClient) createTableAndPartition(ctx context.Context, table Table) error {
	if exists, err := c.checkTableExists(ctx, table.Name); err != nil {
		return err
	} else if exists {
		return nil
	}

	if _, err := c.Exec(ctx, table.getCreateStatement()); err != nil {
		return err
	}

	start, end := getPartitionRange(time.Now())
	partSuffix := start.Format("2006_01_02")
	rangeStart, rangeEnd := start.Format("2006-01-02"), end.Format("2006-01-02")
	_, err := c.Exec(ctx, table.getCreatePartitionStatement(partSuffix, rangeStart, rangeEnd))
	return err
}

func (c *DBClient) createTables(ctx context.Context) error {
	if err := c.createTableAndPartition(ctx, auditLogEventsTable); err != nil {
		return err
	}

	if err := c.createTableAndPartition(ctx, requestInfoTable); err != nil {
		return err
	}

	return nil
}

// InitDBTables Creates tables in the DB.
func (c *DBClient) InitDBTables(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	return c.createTables(ctx)
}

// InsertEvent inserts audit event in the DB.
func (c *DBClient) InsertEvent(ctx context.Context, eventTime time.Time, logData string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	const insertAuditLogEvent QTemplate = `INSERT INTO %s (event_time, log) VALUES ($1, $2);`
	_, err := c.Exec(ctx, insertAuditLogEvent.build(auditLogEventsTable.Name), eventTime, logData)
	if err != nil {
		return err
	}

	return nil
}
