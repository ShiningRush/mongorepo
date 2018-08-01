package mongorepo

import (
	"testing"
	"time"

	"github.com/ShiningRush/dolphin/task"
	"github.com/stretchr/testify/assert"
)

func TestGetAll(t *testing.T) {
	aRepo := NewMongoRepo("mongodb://admin:followme@192.168.8.11:27017?authSource=admin", "crm_datastatistics", "dolphin")
	_, err := aRepo.GetAll()
	assert.NoError(t, err)
}

func TestInsertOrUpdate(t *testing.T) {
	aRepo := NewMongoRepo("mongodb://admin:followme@192.168.8.11:27017?authSource=admin", "crm_datastatistics", "dolphin")
	newStatus := task.TaskStatus{
		TaskName: "test",
		Status: task.Status{
			State:            task.Completed,
			LastExecuteCost:  3,
			LastExecuteState: "testState",
			LastExecuteTime:  time.Now(),
		},
	}
	err := aRepo.InsertOrUpdate(newStatus)
	assert.NoError(t, err)
}

func TestRemoveLegacy(t *testing.T) {
	aRepo := NewMongoRepo("mongodb://admin:followme@192.168.8.11:27017?authSource=admin", "crm_datastatistics", "dolphin")
	m := make(map[string]*task.EtlTask)
	m["test1"] = nil
	m["test2"] = nil

	err := aRepo.RemoveLegacy(m)
	assert.NoError(t, err)
}
