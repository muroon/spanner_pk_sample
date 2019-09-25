package spn

type Option func(*spannerManager)

func SetProjectID(id string) Option {
	return func(sm *spannerManager) {
		sm.projectID = id
	}
}

func SetInstanceID(id string) Option {
	return func(sm *spannerManager) {
		sm.instanceID = id
	}
}

func SetDatabaseID(id string) Option {
	return func(sm *spannerManager) {
		sm.databaseID = id
	}
}
