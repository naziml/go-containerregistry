package registry

import (
	"context"
	"fmt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"sync"
	"zombiezen.com/go/log"
)

type SQLiteManifestStore struct {
	ManifestStore
	lock sync.RWMutex
	ctx  context.Context
	db   *gorm.DB
}

type Manifest struct {
	Repository string `json:"repository"`
	Target     string `json:"target"`
	MediaType  string `json:"mediaType"`
	Blob       []byte `json:"blob"`
}

func (m *Manifest) toManifest() manifest {
	return manifest{
		contentType: m.MediaType,
		blob:        m.Blob,
	}
}

func NewSQLiteManifestStore(dbname string) (*SQLiteManifestStore, error) {
	ctx := context.Background()

	if db, err := gorm.Open(sqlite.Open(dbname), &gorm.Config{}); err != nil {
		log.Errorf(ctx, "Error opening database: %v", err)
		return nil, fmt.Errorf("error opening database: %v", err)
	} else {
		log.Infof(ctx, "Migrating database...")
		db.AutoMigrate(&Manifest{})
		return &SQLiteManifestStore{
			ctx: ctx,
			db:  db,
		}, nil
	}
}

func (m *SQLiteManifestStore) Get(repo string, target string) (*manifest, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	var manifest Manifest
	if err := m.db.Where("repository = ? AND target = ?", repo, target).First(&manifest).Error; err != nil {
		return nil, fmt.Errorf("manifest not found")
	}
	mf := manifest.toManifest()

	return &mf, nil
}

func (m *SQLiteManifestStore) Put(repo string, target string, mf manifest) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	manifest := Manifest{
		Repository: repo,
		Target:     target,
		MediaType:  mf.contentType,
		Blob:       mf.blob,
	}

	if err := m.db.Create(&manifest).Error; err != nil {
		return fmt.Errorf("error creating manifest: %v", err)
	}

	return nil
}

func (m *SQLiteManifestStore) Delete(repo string, target string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.db.Where("repository = ? AND target = ?", repo, target).Delete(&Manifest{}).Error; err != nil {
		return fmt.Errorf("error deleting manifest: %v", err)
	}

	return nil
}

func (m *SQLiteManifestStore) GetTags(repo string) ([]string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	repoManifests := make([]Manifest, 0)
	if err := m.db.Where("repository = ?", repo).Find(&repoManifests).Error; err != nil {
		return nil, fmt.Errorf("error finding manifests: %v", err)
	}

	tags := make([]string, 0, len(repoManifests))
	for _, manifest := range repoManifests {
		tags = append(tags, manifest.Target)
	}

	return tags, nil
}

func (m *SQLiteManifestStore) Exists(repo string, target string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if err := m.db.Where("repository = ? AND target = ?", repo, target).First(&Manifest{}).Error; err != nil {
		return false
	}
	return true
}

func (m *SQLiteManifestStore) ListRepositories() []string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	repos := make([]string, 0)
	if err := m.db.Model(&Manifest{}).Select("repository").Group("repository").Find(&repos).Error; err == nil {
		return repos
	} else {
		return nil
	}
}

func (m *SQLiteManifestStore) ManifestsForRepository(repo string) (map[string]manifest, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	repoManifests := make([]Manifest, 0)
	if err := m.db.Where("repository = ?", repo).Find(&repoManifests).Error; err != nil {
		return nil, false
	} else {
		manifests := make(map[string]manifest, len(repoManifests))
		for _, m := range repoManifests {
			manifests[m.Target] = m.toManifest()
		}
		return manifests, true
	}
}

var _ = (ManifestStore)((*SQLiteManifestStore)(nil))
