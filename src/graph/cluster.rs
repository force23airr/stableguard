use sqlx::PgPool;
use std::collections::HashMap;

/// Union-Find data structure for wallet clustering.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]); // path compression
        }
        self.parent[x]
    }

    fn union(&mut self, x: usize, y: usize) {
        let rx = self.find(x);
        let ry = self.find(y);
        if rx == ry {
            return;
        }
        // Union by rank
        if self.rank[rx] < self.rank[ry] {
            self.parent[rx] = ry;
        } else if self.rank[rx] > self.rank[ry] {
            self.parent[ry] = rx;
        } else {
            self.parent[ry] = rx;
            self.rank[rx] += 1;
        }
    }
}

/// Re-cluster wallets on a chain based on graph edges.
/// Two wallets are in the same cluster if there is a bidirectional edge between them
/// (A sent to B AND B sent to A) which suggests common ownership.
///
/// This is a periodic background operation, not meant to run on every block.
pub async fn recluster(pool: &PgPool, chain_id: i64) -> eyre::Result<u64> {
    // Fetch all bidirectional edges (A→B and B→A both exist)
    let edges: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT e1.source_address, e1.dest_address
         FROM wallet_graph_edges e1
         JOIN wallet_graph_edges e2
           ON e1.source_address = e2.dest_address
          AND e1.dest_address = e2.source_address
          AND e1.chain_id = e2.chain_id
         WHERE e1.chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;

    if edges.is_empty() {
        return Ok(0);
    }

    // Build address → index mapping
    let mut address_to_idx: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut idx_to_address: Vec<Vec<u8>> = Vec::new();

    for (src, dst) in &edges {
        if !address_to_idx.contains_key(src) {
            let idx = idx_to_address.len();
            address_to_idx.insert(src.clone(), idx);
            idx_to_address.push(src.clone());
        }
        if !address_to_idx.contains_key(dst) {
            let idx = idx_to_address.len();
            address_to_idx.insert(dst.clone(), idx);
            idx_to_address.push(dst.clone());
        }
    }

    // Run union-find
    let mut uf = UnionFind::new(idx_to_address.len());
    for (src, dst) in &edges {
        let src_idx = address_to_idx[src];
        let dst_idx = address_to_idx[dst];
        uf.union(src_idx, dst_idx);
    }

    // Extract clusters: root → cluster_id
    let mut root_to_cluster: HashMap<usize, i64> = HashMap::new();
    let mut next_cluster_id = 1i64;

    // Clear existing clusters for this chain and write new ones
    sqlx::query("DELETE FROM wallet_clusters WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;

    let mut count = 0u64;
    for (idx, address) in idx_to_address.iter().enumerate() {
        let root = uf.find(idx);
        let cluster_id = *root_to_cluster.entry(root).or_insert_with(|| {
            let id = next_cluster_id;
            next_cluster_id += 1;
            id
        });

        sqlx::query(
            "INSERT INTO wallet_clusters (address, chain_id, cluster_id)
             VALUES ($1, $2, $3)
             ON CONFLICT (address, chain_id) DO UPDATE SET cluster_id = $3, assigned_at = NOW()",
        )
        .bind(address)
        .bind(chain_id)
        .bind(cluster_id)
        .execute(pool)
        .await?;

        count += 1;
    }

    tracing::info!(
        chain_id,
        wallets = count,
        clusters = root_to_cluster.len(),
        "Reclustered wallets"
    );

    Ok(count)
}
