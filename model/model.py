"""The cost model: experts that predict resource demands, then formulas for runtime, money and energy.

    x (operator, implementation, platform, config, context) --> encoder + per-platform adapter --> h
    for each resource r in CPU core-s, I/O GB, network GB, memory GB:
        demand_r = a_r(h) * wayang_r  +  sum_k b_rk(h) * basis_k(n_in, n_out)          a, b >= 0
    runtime = overhead(h) + soft-max_p( CPU / effective cores, I/O / bandwidth, network / bandwidth )
              * (1 + memory pressure)
    energy  = idle power * runtime + joules per core-s * CPU
    money   = runtime * price per ms                                                (formula, no training)

The row counts reach the model only through the basis, never through the network, and every step is
non-decreasing in the demands, so more rows can never give a lower prediction. With a = 1 and b = 0 a
resource is exactly Wayang's estimate; training learns the correction.
"""
import math

import torch
import torch.nn as nn
import torch.nn.functional as F

from features import CATEGORIES, CFG_COLS, CTX_COLS, N_BASIS, RESOURCES

EMB = {"operator": 16, "impl": 16, "platform": 8, "gc": 4, "source": 2}
ADAPTER = 16
# Size of one unit of each basis coefficient: demand per million rows (or per operator for the constant).
BASIS_UNIT = {"cpu": 1.0, "io": 0.1, "net": 0.1, "mem": 0.1}


def inv_softplus(y):
    return math.log(math.expm1(y))


class CostModel(nn.Module):
    def __init__(self, vocab_sizes, hidden=64, dropout=0.1, wayang_only=()):
        super().__init__()
        self.vocab_sizes = dict(vocab_sizes)
        self.hidden = hidden
        # Resources without training data (e.g. network on one machine) use Wayang's estimate unchanged.
        self.wayang_only = sorted(wayang_only)
        self.emb = nn.ModuleDict({c: nn.Embedding(vocab_sizes[c], EMB[c]) for c in CATEGORIES})
        n_plat = vocab_sizes["platform"]
        # Per-platform adapter: each platform has its own linear map of its config numbers.
        self.adapter_w = nn.Parameter(torch.randn(n_plat, len(CFG_COLS), ADAPTER) * 0.1)
        self.adapter_b = nn.Parameter(torch.zeros(n_plat, ADAPTER))
        d_in = sum(EMB.values()) + ADAPTER + len(CTX_COLS)
        self.encoder = nn.Sequential(
            nn.Linear(d_in, hidden), nn.GELU(), nn.Dropout(dropout),
            nn.Linear(hidden, hidden), nn.GELU(),
        )
        # One expert per resource: [a, b_1..b_K]. Start at a = 1 (Wayang) and small b.
        self.experts = nn.ModuleDict({f"{r}_expert": nn.Linear(hidden, 1 + N_BASIS) for r in RESOURCES})  # "cpu" is taken by nn.Module
        for head in self.experts.values():
            nn.init.zeros_(head.weight)
            with torch.no_grad():
                head.bias[0] = inv_softplus(1.0)
                head.bias[1:] = -4.0
        # Runtime head: fixed overhead (s) and parallel efficiency (0..1) of this operator.
        self.runtime_head = nn.Linear(hidden, 2)
        nn.init.zeros_(self.runtime_head.weight)
        with torch.no_grad():
            self.runtime_head.bias[:] = torch.tensor([-4.0, 0.0])
        # Per-platform constants of the formulas (learned from the measurements).
        self.log_bw_io = nn.Parameter(torch.zeros(n_plat))                   # GB/s, starts at 1
        self.log_bw_net = nn.Parameter(torch.full((n_plat,), math.log(0.1)))  # GB/s, starts at 0.1
        self.p_raw = nn.Parameter(torch.zeros(n_plat))                       # p = 1 + softplus: 1 = add, large = max
        self.mem_raw = nn.Parameter(torch.full((n_plat,), -2.0))             # slowdown when memory fills the heap
        self.idle_raw = nn.Parameter(torch.full((n_plat,), inv_softplus(1.0)))   # W
        self.joule_raw = nn.Parameter(torch.full((n_plat,), inv_softplus(5.0)))  # J per core-second

    # --------------------------------------------------------------------------------------
    def encode(self, cat, cfg, ctx):
        parts = [self.emb[c](cat[:, i]) for i, c in enumerate(CATEGORIES)]
        plat = cat[:, CATEGORIES.index("platform")]
        adapted = torch.einsum("nc,nca->na", cfg, self.adapter_w[plat]) + self.adapter_b[plat]
        return self.encoder(torch.cat(parts + [F.gelu(adapted), ctx], dim=1))

    def forward(self, batch):
        cat, basis, wayang, aux = batch["cat"], batch["basis"], batch["wayang"], batch["aux"]
        h = self.encode(cat, batch["cfg"], batch["ctx"])
        plat = cat[:, CATEGORIES.index("platform")]
        out, coef = {}, {}
        for i, r in enumerate(RESOURCES):
            if r in self.wayang_only:
                out[r], coef[r] = wayang[:, i], torch.ones_like(wayang[:, i])
                continue
            z = F.softplus(self.experts[f"{r}_expert"](h))
            a, b = z[:, 0], z[:, 1:] * BASIS_UNIT[r]
            out[r] = a * wayang[:, i] + (b * basis).sum(1)
            coef[r] = a

        threads, heap_gb, price = aux[:, 0], aux[:, 1], aux[:, 2]
        rt = self.runtime_head(h)
        overhead = F.softplus(rt[:, 0]) * 0.1                      # seconds
        eff = torch.sigmoid(rt[:, 1])
        t_cpu = out["cpu"] / (1 + eff * (threads - 1))
        t_io = out["io"] / torch.exp(self.log_bw_io[plat])
        t_net = out["net"] / torch.exp(self.log_bw_net[plat])
        p = 1 + F.softplus(self.p_raw[plat])
        t = torch.stack([t_cpu, t_io, t_net], 1).clamp_min(1e-9)
        core = torch.exp(torch.logsumexp(p[:, None] * torch.log(t), 1) / p)   # (sum t^p)^(1/p)
        pressure = F.softplus(self.mem_raw[plat]) * F.relu(out["mem"] / heap_gb.clamp_min(1e-3) - 0.5)
        out["runtime"] = overhead + core * (1 + pressure)
        out["energy"] = F.softplus(self.idle_raw[plat]) * out["runtime"] + F.softplus(self.joule_raw[plat]) * out["cpu"]
        out["money"] = out["runtime"] * 1000 * price
        out["wayang_factor"] = coef                                # a_r: how much Wayang's estimate is scaled
        return out

    def platform_constants(self, platforms):
        """Learned formula constants per platform, for the report."""
        res = {}
        with torch.no_grad():
            return self._platform_constants(platforms, res)

    def _platform_constants(self, platforms, res):
        for name, i in platforms.items():
            if name == "<unk>":
                continue
            res[name] = {
                "io_bandwidth_gb_s": float(torch.exp(self.log_bw_io[i])),
                "network_bandwidth_gb_s": float(torch.exp(self.log_bw_net[i])),
                "p (1=add, large=max)": float(1 + F.softplus(self.p_raw[i])),
                "memory_slowdown": float(F.softplus(self.mem_raw[i])),
                "idle_power_w": float(F.softplus(self.idle_raw[i])),
                "joules_per_core_s": float(F.softplus(self.joule_raw[i])),
            }
        return res

    def row_parameters(self):
        """Parameters with one row per category value (dim 0), as {parameter: category}.

        Few-shot training of a new platform trains only these, and only the rows of new values: the
        embeddings of new platforms / implementations / operators, the new platform's adapter and constants.
        The shared encoder and experts stay fixed, so old platforms keep exactly their predictions.
        """
        res = {self.emb[c].weight: c for c in CATEGORIES}
        for p in (self.adapter_w, self.adapter_b, self.log_bw_io, self.log_bw_net, self.p_raw, self.mem_raw,
                  self.idle_raw, self.joule_raw):
            res[p] = "platform"
        return res
