package registry

import (
	"fmt"
	"log"
	"sync"
)

type ManifestMemoryStore struct {
	ManifestStore
	lock      sync.RWMutex
	log       *log.Logger
	manifests map[string]map[string]Manifest
}

func NewInMemoryManifestStore(log *log.Logger) *ManifestMemoryStore {
	return &ManifestMemoryStore{
		log:       log,
		manifests: make(map[string]map[string]Manifest, 10),
	}
}

func (m *ManifestMemoryStore) Get(repo string, target string) (*Manifest, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if mfs, ok := m.manifests[repo]; ok {
		if manifest, ok := mfs[target]; ok {
			return &manifest, nil
		} else {
			return nil, fmt.Errorf("manifest not found")
		}
	}
	return nil, fmt.Errorf("repo not found")
}

func (m *ManifestMemoryStore) Put(mf Manifest) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	repo := mf.Repository
	target := mf.Target

	if _, ok := m.manifests[repo]; !ok {
		m.manifests[repo] = make(map[string]Manifest, 2)
	}

	// Allow future references by target (tag) and immutable digest.
	// See https://docs.docker.com/engine/reference/commandline/pull/#pull-an-image-by-digest-immutable-identifier.
	m.manifests[repo][target] = mf
	return nil
}

func (m *ManifestMemoryStore) Delete(repo string, target string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.manifests[repo]; ok {
		delete(m.manifests[repo], target)
	}
	return nil
}

func (m *ManifestMemoryStore) GetTags(repo string) ([]string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if manifest, ok := m.manifests[repo]; ok {
		tags := make([]string, 0, len(manifest))
		for tag := range manifest {
			tags = append(tags, tag)
		}
		return tags, nil
	}
	return nil, fmt.Errorf("manifest not found")
}

func (m *ManifestMemoryStore) Exists(repo string, target string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if _, ok := m.manifests[repo]; ok {
		if _, ok := m.manifests[repo][target]; ok {
			return true
		}
	}
	return false
}

func (m *ManifestMemoryStore) ListRepositories() []string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	repos := make([]string, 0)
	for repo := range m.manifests {
		repos = append(repos, repo)
	}
	return repos
}

func (m *ManifestMemoryStore) ManifestsForRepository(repo string) ([]Manifest, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if manifests, ok := m.manifests[repo]; ok {
		result := make([]Manifest, 0)

		for _, mf := range manifests {
			result = append(result, mf)
		}

		return result, true
	} else {
		return nil, false
	}
}

var _ = (ManifestStore)((*ManifestMemoryStore)(nil))
