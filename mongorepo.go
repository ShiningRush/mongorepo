package mongorepo

import (
	"github.com/ShiningRush/dolphin/task"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type MongoRepo struct {
	cltName string
	connStr string
	dbName  string
}

func NewMongoRepo(connStr string, dbName string, cltName string) MongoRepo {
	m := MongoRepo{
		cltName: cltName,
		dbName:  dbName,
		connStr: connStr,
	}

	return m
}

func (m MongoRepo) GetAll() ([]task.TaskStatus, error) {
	session, err := mgo.Dial(m.connStr)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	clts := session.DB(m.dbName).C(m.cltName)

	result := []task.TaskStatus{}
	if err = clts.Find(bson.M{}).All(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (m MongoRepo) InsertOrUpdate(ts task.TaskStatus) error {
	session, err := mgo.Dial(m.connStr)
	if err != nil {
		return err
	}
	defer session.Close()
	clts := session.DB(m.dbName).C(m.cltName)

	var exsistedTaskStatus []*task.TaskStatus
	if err := clts.Find(bson.M{"taskname": ts.TaskName}).All(&exsistedTaskStatus); err != nil {
		return err
	}

	if len(exsistedTaskStatus) > 0 {
		clts.Update(
			bson.M{"taskname": exsistedTaskStatus[0].TaskName},
			bson.M{"$set": bson.M{
				"status": ts.Status,
			},
			})
	} else {
		if err := clts.Insert(ts); err != nil {
			return err
		}
	}

	return nil
}

func (m MongoRepo) RemoveLegacy(newTs map[string]*task.EtlTask) error {
	session, err := mgo.Dial(m.connStr)
	if err != nil {
		return err
	}
	defer session.Close()
	clts := session.DB(m.dbName).C(m.cltName)

	totalCount, err := clts.Count()
	if err != nil {
		return err
	}

	if totalCount > 0 {
		taskNames := m.changeToTaskNameArray(newTs)

		if _, err = clts.RemoveAll(bson.M{
			"taskname": bson.M{"$not": bson.M{"$in": taskNames}},
		}); err != nil {
			return err
		}
	}

	return nil
}

func (m MongoRepo) changeToTaskNameArray(newTs map[string]*task.EtlTask) []string {
	taskNames := []string{}
	for k := range newTs {
		taskNames = append(taskNames, k)
	}

	return taskNames
}
