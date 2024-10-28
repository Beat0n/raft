package shardctrler

type group struct {
	gid    int
	shards []int
}

func (g *group) add(cfg *Config, shardIds ...int) {
	g.shards = append(g.shards, shardIds...)
	for _, shardId := range shardIds {
		cfg.Shards[shardId] = g.gid
	}
}

func (g *group) remove(shardId int) {
	for i, id := range g.shards {
		if id == shardId {
			g.shards = append(g.shards[:i], g.shards[i+1:]...)
		}
	}
}

func (g *group) pop() int {
	shardID := g.shards[0]
	g.shards = g.shards[1:]
	return shardID
}
