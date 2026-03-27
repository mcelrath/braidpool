# Braid Consensus

Herein we describe the Braid consensus mechanism, which is a generalization of
Nakamoto consensus to a Directed Acyclic Graph (DAG).

If the math in this document isn't rendering correctly, ensure that you have the 
Latin Modern fonts installed on your system.

## Braid Structure

The Braid is a DAG structure where each node (bead) may have one or more
parents. It has one additional rule ("no incest") that one cannot name an
ancestor of a parent as a parent. This eliminates sub-graphs that contain
triangles. The reason for this extra rule is that there is no additional
information conveyed by naming a higher-order ancestor as a parent. Parents of
parents (and all other ancestors) are already considered.

An example of a "thin" braid is:

[thin-braid]: thin_braid.png
<a id="thin-braid">

![Thin Braid][thin-braid]

</a>

Here time is increasing as we move right.  The "no incest" rule means that for
example, beads 8 and 9 cannot name beads 0-6 as direct parents. The colors
correspond to "cohorts" which are sub-graphs separated by graph cuts. A graph
cut is a line drawn through the graph where *all* beads on the right side of the
cut have *all* beads on the left side of the cut as ancestors. The braid tip in
this example is the bead (10), which is expected to be named as the sole parent
by a miner starting from this graph state.

An example of a "thick" braid is:

[thick-braid]: thick_braid.png
<a id="thick-braid">

![Thick Braid][thick-braid]

</a>

In this image we can see an example of a higher order graph cut between cohort
(1,2,3) and cohort (4,5,6,7,8). The tips in this case are the beads (40,41),
both of which should be named as parents of a miner starting from this graph
state.

The highest work path is indicated by the thick arrows running through the
middle of the graph, and beads away from the highest work path have decreasing
work as you move away from the path.  The work of each bead is the *descendant*
work, with ancestor work being used as a tie-breaker. By using descendant work,
we incentivize miners to broadcast their beads quickly so that they collect
descendants.

Graph cuts can be found with high speed using a depth first search and the
[Lowest Common Ancestor](https://en.wikipedia.org/wiki/Lowest_common_ancestor)
algorithm, which can be computed in linear time.

## Braid Mathematics

The production of Proof of Work shares is a Poisson process, given by the
Poisson probability mass function which gives the probability mass that $k$
beads are formed within a time $t$ assuming constant hashrate $\lambda$ and difficulty $x$:

<a id="1"></a>

$$\tag{1}
\begin{align}
P(t,k) = \frac{(\lambda x t)^k e^{-\lambda x t}}{k!}
\end{align}
$$

where the parameter $\lambda$ is the total hashrate of the network having units
[hashes/second], $t$ has units [seconds], and $x$ is unitless.

For any subgraph corresponding to a length of time $T$, we can *measure* the
number of beads $N_B$, the number of cohorts $N_C$ as well as the average time
per bead $T_B = T/N_B$ and average time per cohort $T_C = T/N_C$. Finally the
quantity $x$ is the "target difficulty" representing the maximum acceptable
value for a proof of work hash. This gives the hashrate as:

<a id="2"></a>

$$\tag{2}
\begin{align}
\lambda = \frac{N_B}{xT}
\end{align}
$$

The cohort time $T_C$ is easy to understand in the two limits $x\to0$ (high
difficulty - blockchain-like) and $x\to \infty$ (low difficulty - thick braid).
In the $x\to0$ limit, no beads have multiple parents, and each bead is a cohort.
The cohort time is then:

<a id="3"></a>

$$\tag{3}
\begin{align}
T_C|_{x\to0} = T_B = \frac{1}{\lambda x}.
\end{align}
$$

In the opposite limit, in order to form a cohort, no beads must be produced
within a time approximately $a$ such that all beads have time to propagate to
other nodes, and be named as parents for the next bead(s), creating a cohort.
Here $a$ is the network propagation latency in seconds.  The probability that no beads are created
within a time interval $a$ is given by

<a id="4"></a>

$$\tag{4}
\begin{align}
P(a,0) = e^{-\lambda x a}.
\end{align}
$$

On average within a window $T$ we want $a$ to be our latency parameter satisfying:

<a id="5"></a>

$$\tag{5}
\begin{align}
T P(a,0) = a.
\end{align}
$$

Rearranging this using $T=T_CN_C$ and $N_C=1$:

<a id="6"></a>

$$\tag{6}
\begin{align}
T_C|_{x\to\infty} = \frac{a}{P(a,0)} = a e^{\lambda x a}
\end{align}
$$

Taken together, the cohort time is well approximated by the sum of these
two limiting contributions (Eqs.[3](#3),[6](#6)):

<a id="7"></a>

$$\tag{7}
\begin{align}
T_C = \frac{1}{\lambda x} + a e^{a\lambda x}
\end{align}
$$

This formula has been validated by extensive simulation against a 25-node
network with realistic latencies (see `tests/Consensus Figures.ipynb`). The
curve fit to the simulated data matches Eq.[7](#7) over more than four orders
of magnitude in $x$. The exact behavior near the minimum depends on the network
topology and inter-node latencies, but the functional form is robust.

In the blockchain limit ($x\to0$) the first term dominates. In the thick-braid
limit ($x\to\infty$) the second term dominates. The sum interpolates between
these regimes.

![Cohort Time vs target difficulty](T_C_x.png)

### Cohort Size Distribution

We define the *beads-per-cohort ratio* $R = N_B/N_C$. From Eq.[7](#7) the
expected ratio is

<a id="R"></a>

$$\tag{R}
\begin{align}
R = \lambda x \cdot T_C = 1 + z e^z
\end{align}
$$

where $z = a\lambda x$ is the dimensionless *braid density parameter*.

In simulation, the number of beads in a single cohort follows a **geometric
distribution** with effective parameter

$$
p_{\rm eff} = \frac{1}{R} = \frac{1}{1 + z e^z} = \frac{N_C}{N_B}
$$

For cohort $j$ containing $k_j$ beads:

<a id="cohort-dist"></a>

$$\tag{CD}
P(k_j \mid p_{\rm eff}) = p_{\rm eff}(1-p_{\rm eff})^{k_j - 1}, \qquad k_j = 1, 2, 3, \ldots
$$

The effective parameter $p_{\rm eff}$ is not the Poisson gap probability $e^{-z}$. In a
single-Poisson-stream model, a cohort closes whenever the inter-arrival gap
exceeds $a$, giving $p = e^{-z}$ and $\mathbb{E}[R] = e^z$. In a multi-node
network, a graph cut requires that *all* nodes have received *all* beads, which
is a stronger condition: a gap in the aggregate arrival process may not produce
a graph cut if some bead is still propagating. This makes cohorts "stickier"
and $p_{\rm eff} < e^{-z}$.

The relationship $p_{\rm eff} = 1/(1+ze^z)$ and the geometric distribution
([CD](#cohort-dist)) are validated by simulation of a 25-node network with
realistic propagation delays (see `tests/Consensus Figures.ipynb`). Both the
curve fit $R = 1 + ze^z$ and the histogram of single-cohort sizes (which should
be a straight line on a log scale with slope $\log(1-p_*)$) confirm this model.
The geometric assumption should be verified for different network topologies; if
the true distribution deviates from geometric, the Beta-conjugate update remains
a consistent estimator of the mean $p$ but is no longer exactly conjugate.

The sufficient statistic for estimating $p_{\rm eff}$ from $N_C$ cohorts
containing $N_B$ total beads is simply $(N_C, N_B - N_C)$: the number of
cohort-closing events and non-closing events respectively. The maximum
likelihood estimator is $\hat{p} = N_C/N_B$, and the MLE for $z$ is therefore

<a id="z-mle"></a>

$$\tag{MLE}
\hat{z} = W\!\left(\frac{N_B}{N_C} - 1\right) = W(R - 1)
$$

where $W$ is the Lambert W function. This is the same inversion used throughout
the difficulty adjustment algorithm. Since $W(x) \sim x$ for small $x$,
$\hat{z}$ is well-behaved as $R \to 1^+$. However, the *derivative*
$dW/dx \to 1$ at $x=0$ while $dR/dz = (1+z)e^z \to 1$ at $z=0$, so the
sensitivity $dz/dR = 1/((1+z)e^z) \to 1$ — the estimate is not ill-conditioned
near the blockchain limit, but carries maximal relative uncertainty since
$\sigma_z/z$ diverges as $z \to 0$.

### Variance of $N_B/N_C$

The variance of the beads-per-cohort ratio $R = N_B/N_C$ is needed for
understanding the statistical precision of any difficulty adjustment algorithm.

$N_B$ and $N_C$ are **not independent**: every cohort contains at least one
bead, so $N_B \geq N_C$. We decompose the bead count as $N_B = N_C + M$ where
$M = N_B - N_C$ is the number of non-cohort-starting beads. In the geometric
model ([CD](#cohort-dist)), each cohort independently contributes 1 to $N_C$
and $(k_j - 1)$ to $M$, so $N_C$ and $M$ are sums of independent
contributions from different components of the sufficient statistic, and are
approximately independent for large $N_C$. (This can be verified empirically
from the simulation.) Since $N_B/N_C = 1 + M/N_C$ with $M$ approximately
independent of $N_C$, the delta method applied to the ratio $M/N_C$ gives
(valid for $\mu_C \gg 1$, i.e., observation windows of many cohorts):

<a id="var"></a>

$$\tag{V}
{\rm Var}\!\left[\frac{N_B}{N_C}\right]
= \frac{\mu_B}{\mu_C^2}(R - 1)
$$

where $\mu_B = \mathbb{E}[N_B]$ and $\mu_C = \mathbb{E}[N_C]$ and $R =
\mu_B/\mu_C$. Equivalently:

$$
{\rm Var}\!\left[\frac{N_B}{N_C}\right]
= \frac{\mu_M}{\mu_C^2} + \frac{\mu_M^2}{\mu_C^3}
$$

with $\mu_M = \mu_B - \mu_C$. Since $\mu_M(\mu_C + \mu_M) = \mu_M \mu_B$,
the expanded and compact forms are equal. This has the same
algebraic form as a naive independent-Poisson variance formula, but with
$\mu_M$ (the extra-beads rate) replacing $\mu_B$.

Propagating to the $z$-space variable via the delta method with
$dR/dz = (1+z)e^z$:

<a id="var-z"></a>

$$\tag{Vz}
\sigma_z^2 = \frac{R(R-1)}{\tau_C(1+z)^2 e^{2z}}
\approx \frac{0.29}{\tau_C}
\quad (\text{at the operating point } z_*)
$$

where $\tau_C$ is the number of cohorts in the observation window. A single
7-cohort window gives $\sigma_z/z_* \approx 29\%$ — this is the fundamental
noise floor that any difficulty adjustment algorithm must contend with.

### Operating Point

We may solve Eq.[7](#7) for $a$ by substituting $u = a\lambda x$. Then $T_C -
1/(\lambda x) = (u/\lambda x)e^u$, so $ue^u = \lambda x(T_C - 1/(\lambda x))
= \lambda x T_C - 1 = N_B/N_C - 1$ (using $T_C = T/N_C$ and $\lambda x =
N_B/T$). Therefore $u = W(N_B/N_C - 1)$ and $a = u/(\lambda x) =
(T/N_B)W(N_B/N_C - 1)$:

<a id="8"></a>

$$\tag{8}
\begin{align}
a = \frac{T}{N_B} W\left(\frac{N_B}{N_C}-1\right)
\end{align}
$$

The location of the minimum is given by

<a id="9"></a>

$$\tag{9}
\begin{align}
\frac{\partial T_C}{\partial x}=0
\qquad
\implies
\qquad
x = x_0 = \frac{2 W\left(\frac12\right)}{a\lambda} \simeq \frac{0.7035}{a \lambda}
\end{align}
$$

where $W(z)$ is the [Lambert W
function](https://en.wikipedia.org/wiki/Lambert_W_function).  Using $a$ from
above, the factors of $\lambda$, $a$, and $T$ all cancel out, giving us:

<a id="10"></a>

$$\tag{10}
\begin{align}
1 = \frac{2 W\left(\frac12\right)}{W\left(\frac{N_B}{N_C}-1\right)}
\qquad
\implies
\qquad
\frac{N_B}{N_C} = \frac{W(\frac12)+\frac12}{W(\frac12)} \simeq 2.4215
\end{align}
$$

In steady state (constant hashrate), there are on average 2.42 beads per
cohort. This result is independent of latency $a$, hashrate $\lambda$, and
observation window $T$. The minimum value of $T_C$ in
units of latency $a$ is given by

$$
T_{C,\min}/a = \frac{1}{a\lambda x_0} + e^{a\lambda x_0} =
    \frac{1}{2 W(\frac12)} + e^{2W(\frac12)} \simeq 3.44
$$

This value $x_0$ or equivalently $N_B/N_C \simeq 2.42$ and corresponding
$T_{C,\min} \simeq 3.44\, a$ represents having the most-frequent consensus
points within a global network. Below we will use this ratio to create our
difficulty adjustment algorithm targeting "most-frequent consensus" in a way
that is insensitive to latency $a$, hashrate $\lambda$, and averaging window
$T$.

We present times in units of the latency $a$, because there are many sources of
latency not taken into account in our simulation, including actual transmission
speed in copper or fiber optic cables, the topology of the global network,
processing time of beads and creating block templates, and switching latency in
directing mining devices to change their work unit. Nonetheless our results
indicate that we can devise an algorithm completely insensitive to all these
sources of latency, and completely independent of timestamps which
have been a source of manipulation on other blockchains. It will operate as fast
as it possibly can, given the (measured) latency constraints, and automatically
adjust to changing network conditions and hashrate. We anticipate that the
latency from all sources will be on the order of 100–200ms, resulting in a bead
rate around 500ms, resulting in approximately 1000 beads (shares) per bitcoin
block.

Given any $x$, we can determine how far we are from the desired
target $x_0$ and $N_B/N_C=2.42$ by making a ratio which cancels out the factors
of $a$ and $\lambda$.
<a id="11"></a>

$$\tag{11}
x_0 = \frac{2\, x\, W\!\left(\frac12\right)}{W\!\left(\frac{N_B}{N_C}-1\right)}
$$

## Consensus

Given a bead with $n$ parents $\{p_i\}$, $i=1..n$, any computable quantity
$f(\{p_i\})$ can be decided simply by examining the parents $p_i$ and their
ancestors. Thus just as in Bitcoin, any state can be determined solely by
knowing the braid tips.

The majority of consensus considerations in Bitcoin are regarding acceptable
transactions. As the first version of Braidpool will not have transactions, that
leaves the target difficulty for shares as the only quantity that needs to be
decided by consensus, which we describe how to compute below.

### Bead Timestamps

Difficulty adjustment algorithms historically have a problem with miner-attested
timestamps being placed too far in the future, and often reject blocks with
times too far in the future. A number of timing-based attacks are possible by
manipulating timestamps in this way.

Instead of relying on the miner's attested timestamps, we will instead use
*observer* timestamps. Whenever a node receives a bead, it records a timestamp
of when that bead was observed. The [committed
metadata](https://github.com/braidpool/braidpool/blob/main/docs/braidpool_spec.md#metadata-commitments)
will contain not only the miner's timestamp indicating when they started
mining this bead, but timestamps of each bead in his parent cohort, the
parent's-parent cohort, and the parent's-parent's-parent cohort (as observed
from this bead). This gives us a minimum of three observations of the
*received* timestamp for each bead. The `median_bead_time` for a bead is then
taken to be the median of these observations. In the following all references to
bead timestamps refer to this `median_bead_time`.

Because of the necessity to have at least 3 measurements of bead time for each
bead, when computing $T_B$ and $T_C$, we exclude the three most recent ancestor
cohorts (as observed from the bead under consideration, including that bead as
the final cohort) as there aren't enough observations of their received time to
evaluate the bead time. Therefore the observation window $T$ starts 3 cohorts
back and extends backwards in time for an interval $T$.

In order to pull off any timing-based attacks, a miner would need to control on
average 2 of these 3 timestamps, which is anyway a 51% attack and the system
breaks down anyway.

We can also compute the average solve time by comparing the miner's timestamp to
this observer timestamp. This may be used in a future update of this algorithm
to detect misbehaving miners.

## Difficulty Adjustment Algorithm

It is necessary to rate-limit shares. Since shares are broadcast to all nodes,
if the bead rate is too high, the communications complexity of sending all
shares with $N$ miners is $\mathcal{O}(N^2)$, which can easily be too much
bandwidth to handle.  If the bead rate is too high, one can
effectively never have graph cuts. This is because in order for a graph cut to
occur, the network must be quiescent for a time proportional to $a$.

### Bayesian Difficulty Adjustment

The difficulty adjustment problem is primarily an **estimation problem** — the
dominant challenge is extracting the signal $s = a\lambda$ from noisy cohort
observations, not designing a feedback controller. The unknown $s$ changes when the
hashrate or network topology changes. The target difficulty $x$ is determined
from the estimate of $s$ via $x = z_*/s$ where $z_* = W(R_* - 1) \simeq
0.7035$.

Each cohort's bead count carries information about $s$ via the geometric
likelihood $P(k_j \mid s)$ with parameter $p_{\rm eff}(s, \bar{x}_j) = 1/(1 +
s\bar{x}_j \exp(s\bar{x}_j))$, where $\bar{x}_j$ is the harmonic mean target
of beads in cohort $j$. Because $\bar{x}_j$ is known data (not estimated),
changing the target does not corrupt the likelihood, and the feedback loop that
causes PID ringing is broken.

**Beta-conjugate formulation.** For a window of cohorts with approximately
constant target $\bar{x}$, the parameter $p_{\rm eff}$ can be estimated
directly. The conjugate prior is $p \sim \text{Beta}(\alpha_0, \beta_0)$, and
after observing a cohort with $k_j$ beads the posterior parameters update as:

$$
\alpha \leftarrow \alpha + 1, \qquad \beta \leftarrow \beta + (k_j - 1)
$$

After $N_C$ cohorts containing $N_B$ total beads, the posterior is:

$$
p \mid \text{data} \sim \text{Beta}(\alpha_0 + N_C,\; \beta_0 + N_B - N_C)
$$

The posterior mean is $\hat{p} = (\alpha_0 + N_C)/(\alpha_0 + \beta_0 + N_B)$,
which for a Jeffreys prior ($\alpha_0 = \beta_0 = 1/2$) converges quickly to
$N_C/N_B$. The new target is then:

<a id="target-update"></a>

$$\tag{T}
x_{\rm new} = \frac{z_* \bar{x}}{W(1/\hat{p} - 1)}
$$

This update is deterministic given the DAG: all nodes examining the same
ancestors compute the same $\hat{p}$ and hence the same target. The committed
metadata contains this target, which is verified by all nodes.

### Bayesian Online Changepoint Detection (BOCPD)

The product $s = a\lambda$ can change discontinuously when miners
connect/disconnect or network topology changes. Rather than choosing a fixed
observation window $\tau_C$, we use Bayesian Online Changepoint Detection
(Adams & MacKay, 2007) which automatically adapts the effective window.

The algorithm maintains a distribution over *run lengths* $r$ — how many
cohorts since the last changepoint. For each run length, a separate Beta
posterior on $p$ is maintained:

**Per cohort $j$ with $k_j$ beads:**

1. **Predict.** For each run $r$, compute the Beta-Geometric predictive
   probability:
   $$\pi_r = \frac{\alpha_r}{\alpha_r + \beta_r} \prod_{i=0}^{k_j - 2}
   \frac{\beta_r + i}{\alpha_r + \beta_r + 1 + i}$$

2. **Update run lengths.**
   - Growth: $w_{r+1} \leftarrow w_r \cdot \pi_r \cdot (1-h)$
   - Changepoint: $w_0 \leftarrow \sum_r w_r \cdot \pi_r \cdot h$
   - Normalize weights.

3. **Update posteriors.** For each surviving run:
   $\alpha_r \leftarrow \alpha_r + 1$, $\beta_r \leftarrow \beta_r + (k_j - 1)$.

4. **Prune.** Remove runs with $w_r < 2^{-64} \cdot w_{\max}$.

5. **Estimate.** Marginal posterior mean:
   $$\hat{p} = \sum_r \tilde{w}_r \cdot \frac{\alpha_r}{\alpha_r + \beta_r}$$
   where $\tilde{w}_r$ are normalized weights. Then apply Eq.([T](#target-update)).

**Initial state.** At genesis (or after a hard-coded initial difficulty period),
the BOCPD state consists of a single run $r=0$ with prior
$\text{Beta}(\alpha_0, \beta_0) = \text{Beta}(1/2, 1/2)$ (Jeffreys prior) and weight
$w_0 = 1$.

**Hazard rate.** The hazard rate $h$ is the prior probability of a changepoint
per cohort. We maintain $H$ candidate values $h_i \in \{1/50, 1/100, 1/500,
1/2000\}$ with initial weights $\omega_i = 1/H$. At each cohort, each
candidate maintains its own set of run-length weights $\{w_r^{(i)}\}$. The
marginal predictive probability for candidate $h_i$ is:
$$\pi(h_i) = \sum_r w_r^{(i)} \cdot \pi_r$$
where $\pi_r$ is the Beta-Binomial predictive from step 1 (which depends only
on the Beta parameters, not on $h$). The candidate weight is then updated:
$\omega_i \leftarrow \omega_i \cdot \pi(h_i)$, followed by normalization
$\omega_i \leftarrow \omega_i / \sum_j \omega_j$. The effective hazard rate
is $h_{\rm eff} = \sum_i \omega_i h_i$. This makes $h$ self-tuning.

Properties of the BOCPD:

The algorithm automatically weights multiple time horizons: short runs (recent
changepoint, wide posterior) provide fast response, while long runs (stable
estimate, narrow posterior) provide precision. Unlike PID-based approaches that
require choosing $\tau_C = 7$ or 19 or 102, BOCPD discovers the effective
window from the data.

In steady state, most weight concentrates on one long run. Runs from before the
last changepoint decay at rate $(1-h)^n$ per step; with $h = 1/100$, runs older
than ~6400 cohorts (~30 minutes) have weight below $2^{-64}$.

When a cohort has $k = 1$ bead, the likelihood is simply $p_{\rm eff}$, an
informative observation that pushes the posterior toward large $p$ (low $z$,
blockchain-like). No special handling is needed.

The state consists of $O(1/h)$ Beta pairs per $h$-candidate, $\approx 400$
pairs of integers total with 4 candidates. The algorithm processes whole
cohorts atomically: the observation from cohort $j$ is the total bead count
$k_j$. Beads within a cohort are unordered for purposes of the update.

For the BOCPD, the harmonic mean target $\bar{x}$ in Eq.([T](#target-update))
is computed over the parents of the new bead ($\bar{x}_{\rm parents}$), the
same quantity used in all other difficulty adjustment algorithms. Each cohort's
likelihood uses the harmonic mean target of beads within that cohort.

All Beta-Geometric predictive probabilities are products of rational numbers
with small numerators and denominators (since $k_j$ is typically 1-10).
Log-weights can be stored as fixed-point integers. The Lambert W evaluation in
Eq.([T](#target-update)) can use a precomputed lookup table indexed by the
rational $\hat{p}$.

The committed metadata contains the target $x$ and a millisecond-resolution
timestamp which is required to be monotonic. A bead's timestamp must be
strictly greater than that of any of its parents. This timestamp is *different*
from the timestamp in the Bitcoin block header, which is commonly used as nonce
space for mining and not accurate. All time-dependent calculations herein use
this timestamp, not the Bitcoin block header timestamp.

### Monotone Convergence and Damping

The classical difficulty adjustment feedback loop works as follows: observe the
braid over a window, compute an error, adjust the target. This creates a
feedback cycle where the adjusted target affects future measurements, which
drive further adjustments. With a fixed window $T$, this cycle oscillates with
period $\sim 2T$ and requires explicit damping (e.g., a PID derivative term) to
prevent ringing.

The Bayesian approach achieves monotone convergence without any free damping
parameter, for two reasons:

1. *The feedback loop is attenuated.* Because we estimate $s = a\lambda$
rather than $z = a\lambda x$, and divide out the known target $\bar{x}$ from
each cohort's likelihood, the *primary* cause of DAA oscillation — the
controller confusing its own action for a change in the environment — is
greatly reduced. A residual coupling exists: if $x$ overshoots by a factor $f$,
then $z_{\rm obs} = s \cdot x_f$ is larger, giving $\hat{s} = \hat{z}/x_f$.
The division by the known $x_f$ correctly cancels the overshoot from the
estimate. The feedback is broken to first order; second-order effects (such as
the geometric model depending on $z$ rather than on $s$ and $x$ separately)
are small and bounded by $O(\sigma_z^2)$.

2. *The posterior mean cannot overshoot.* After a step change
$s_{\rm old} \to s_{\rm new}$, the Bayesian posterior mean converges
monotonically from $s_{\rm old}$ toward $s_{\rm new}$:

$$
\hat{s}(n) = \frac{a_0 \hat{s}_{\rm prior} + n \hat{s}_{\rm MLE}}{a_0 + n}
$$

This is a weighted average that moves toward the MLE as data accumulates.
It cannot exceed the MLE, and the MLE itself converges to $s_{\rm new}$.
Individual cohorts cause zero-mean fluctuations around this trajectory, but
because there is no feedback, these fluctuations do not compound into
oscillation.

The posterior mean is the minimum-variance estimator with the monotone
convergence property. Note that while $\hat{p}$ converges monotonically, the
target $x_{\rm new} = z_* \bar{x}/W(1/\hat{p}-1)$ is a nonlinear
transformation; since $W$ is concave and monotone increasing, and $1/\hat{p}-1$
is monotone decreasing in $\hat{p}$, the composition preserves monotonicity of
$x$ with respect to $\hat{p}$. Thus the *target itself* cannot overshoot.

*Step response after a changepoint.* When $s$ jumps discontinuously:

1. The BOCPD detects the changepoint (old long-run posterior assigns low
   likelihood to new observations).
2. A new short run is created with a wide Jeffreys prior.
3. Over the next $\sim$5–20 cohorts, the new run accumulates data and its
   posterior concentrates on $s_{\rm new}$.
4. The long-run posterior's weight decays; the short run dominates.
5. The marginal estimate $\hat{s}$ moves smoothly to $s_{\rm new}$.

The "settling time" depends on the magnitude of the change. For a fractional
change $f$ in $s$, the BOCPD requires enough new data that the change exceeds
the posterior uncertainty. The posterior standard deviation after $n$ cohorts
scales as $\sigma_p \sim 1/\sqrt{n}$, so detecting a change of size $f$ in $p$
(with $p \sim 0.41$ at the operating point) requires $n \sim 1/f^2$ cohorts.
Large changes ($f > 0.3$) settle in $\sim$5 cohorts ($\sim$2 seconds at the
operating point). Small changes ($f \sim 0.1$) require $\sim$100 cohorts. This
Within the geometric model class, this is optimal: no algorithm assuming the
same likelihood can respond faster with fewer observations.

Unlike PID-based approaches that require choosing a window $T$ or $\tau_C$, the
BOCPD discovers the effective window from the data via the run-length
distribution. The hazard rate $h$ (self-tuned from a small set of candidates)
is the only parameter, and the algorithm is robust to its value over a
$10\times$ range.

### Edge Cases

#### Blockchain-like: $N_C = N_B$

An edge case occurs when the number of cohorts is the same as the number of
beads. Here the DAG is blockchain-like, with no higher-order structures. As a
consequence, $W(0)=0$ and therefore $a=0$. If this was a blockchain we'd say
that within the time window it had no orphans, and we fail to get a measurement
of the network latency.  This might happen if:

1. The difficulty is too high
2. There's only one miner on the network who has configured synchronous block
template updates for his mine.
3. All miners are geographically centralized very near to each other, giving a
relative latency between mining nodes that is much smaller than that expected of
a global network.

We know that this network is operating on planet Earth which has a fixed size,
and a fixed latency to get a message around the globe. Therefore we can
configure a reasonable minimum $a$ as follows: consider a mining network with 4
mining nodes distributed as a tetrahedron on the surface of the Earth.  This
is the largest number of nodes in which all nodes are directly connected to each
other.  The physical distance between these nodes is the arc length on the
surface of a sphere of radius corresponding to the mean radius of the earth
$r_e=6371\ {\rm km}$. This arc length is $\ell = r_e \arccos(-1/3) = 12,173\
{\rm km}$ where this angle is approximately $109.47^\circ$.  Assuming signal
propagation can happen at the speed of light (e.g. using satellites) this is a
propagation latency of $a_{\rm min} = \ell/c = 40.60\ {\rm ms}$. We will use
this as a minimum value for $a$.

This will produce a maximum share rate for the network of approximately 24.628
shares per second, corresponding to 14777 shares per bitcoin block. If Braidpool
is used as a proxy server for e.g. a hosting provider with multiple hosts in a
single location, this is the expected share rate in the absence of
customizations for that use case.

#### Bitcoin Block Consensus Rule

A bead that names a Bitcoin block as a parent MUST NOT name any other parents
that are not descended from that same Bitcoin block. This causes the DAG to
collapse to a single point at each Bitcoin block boundary. Any "sibling beads"
to a Bitcoin block would have been orphans had they been Bitcoin blocks
themselves, and cannot contribute to pool revenue, so this is enforced as a
consensus rule.

#### Large Cohorts and Selfish Mining Mitigation

In simulations at constant difficulty (no DAA), cohorts up to about $N_B \approx
25$ beads occur a few times per 2-week Bitcoin DAA epoch (see `tests/Consensus
Figures.ipynb`). The DAA distorts the cohort size distribution but cannot
prevent occasional large cohorts, and should not overreact to them.

For cohorts exceeding a threshold (approximately 25-30 beads), we sort beads
within the cohort by descendant work and refuse to pay the lowest-work beads.
This is a selfish mining mitigation: an attacker who withholds beads and
produces a large cohort will find that the lowest-work beads in that cohort
(which are likely the withheld beads, having fewer descendants) receive no
reward.

This penalty does NOT affect the DAA. The difficulty adjustment is deterministic
given only the parents of the bead being validated. The large-cohort penalty
affects only reward distribution and is expected to trigger rarely in normal
operation. It will trigger on network splits or active selfish mining attacks.

The threshold should be set conservatively above the natural tail of the cohort
size distribution at the operating point, so that honest miners are never
penalized under normal network conditions.

### Difficulty Discussion

Each bead has a different difficulty which is independently computable by all
nodes, and when a share is broadcast, this is verified as a consensus
requirement for the share to be valid.

The Bayesian algorithm automatically adjusts without any further coordination.
If the bead rate is low and the DAG is blockchain-like, the posterior on $p$
moves toward 1, which drives $z \to 0$ and increases $x$ (lowers the
difficulty). If the bead rate is high, the posterior drives $p$ toward small
values, increasing $z$ and decreasing $x$. It does this without oscillating
because the estimation is decoupled from the control action.

*Genesis and startup.* At genesis, a single BOCPD run exists with prior
$\text{Beta}(1/2, 1/2)$. The first beads are all $k=1$ (blockchain-like,
difficulty too high). Each $k=1$ cohort increments $\alpha$ by 1, leaving
$\beta$ unchanged, pushing $\hat{p} \to 1$ and $x$ upward (easier mining).
As soon as multi-bead cohorts appear, $\beta$ starts incrementing and the
system finds the
operating point naturally. No special genesis handling is required.

### Consensus on Other Quantities

For other information that may be contained within beads about which we wish to
reach consensus we require that:

* Children must not have information that conflicts with their parents.
* Beads which are not ancestors of one another *may* contain conflicting
  information (for instance, a double-spend).

Consensus points occur at graph cuts (cohort boundaries).  Because of the above
rules, it is only necessary to decide between conflicting information *within* a
cohort. For example, in the [thin braid](#thin-braid) example, beads (8) and (9)
can contain conflicting information, the resolution of which is decided by a
"merge" rule in the parent bead that ties them together. In Nakamoto consensus
this "merge" rule is work weighting where work $w=1/x$ in terms of the target
difficulty $x$.  In the event that (8) and (9) have exactly the same work, the
bead with the smaller hash is chosen. (a.k.a. "luck") In the event that the
cohort is more complex, descendant work must be taken into account. This is
given by a simple sum of descendant work for bead $i$:

$$
w_i = \frac{1}{x_i} + \sum_{d \in {\rm descendants}} \frac{1}{x_d}
$$

where the bead with the larger work value is preferred to resolve conflicting
information, and the sum need only be carried out until the next cohort
boundary, since by the definition of cohorts, all additional work after the
cohort boundary is added to the work of *all* potentially conflicting beads and
does not affect conflict resolution, and DAGs don't fork.

### Rewards

We would like to reward all beads regardless of graph structure: equal pay for
equal proof-of-work, however there's a limit on the latency we can accept. A
very high latency bead is less likely to contribute to Bitcoin's proof of work
and would create orphans, which do not increase the revenue of the pool.
Parallel work does not contribute to the total proof of work. At the same time
we do not want to create incentives on latency that are so strong that they
encourage geographic centralization or latency and connectivity games as
[occurred with P2Pool](https://bitcointalk.org/index.php?topic=153232.0).

For cohorts below the large-cohort threshold (~25-30 beads), all beads receive
equal reward proportional to their proof-of-work. For cohorts exceeding the
threshold, beads are sorted by descendant work and the lowest-work beads receive
no reward (see "Large Cohorts and Selfish Mining Mitigation" above). This
concentrates rewards on beads that contributed to the pool's Bitcoin proof of
work and penalizes withheld or late beads.

## Alternatives Considered

### PID Controller

Traditional difficulty adjustment algorithms (including Bitcoin's) and early
Braidpool prototypes used PID (Proportional-Integral-Derivative) controllers.
Extensive experiments (`tests/pid_calibrate.py`, `tests/simulator.py` with 13+
DAA variants) showed several problems:

1. The D term amplifies noise. The derivative of $z$ is a difference of two
   noisy measurements with $\sigma \approx 0.54\sqrt{2}/\sqrt{\tau_C}$, worse
   than the measurement itself.

2. The P term chases noise. With 29% noise on $z$ from a 7-cohort window,
   the proportional response mostly reacts to randomness.

3. A PID that estimates $z = a\lambda x$ confuses its own control action
   (changing $x$) with changes in $a\lambda$. This feedback corruption is the
   primary cause of ringing. Empirically, only the I term survives calibration,
   and an integral-only controller is just a running mean.

PID is a generic linear controller. Since the exact likelihood is known, we can
do better.

### Extended Kalman Filter

An Extended Kalman Filter (EKF) on $\log s$ with a random-walk process model
($\log s_{n+1} = \log s_n + w_n$, $w_n \sim N(0,q)$) provides adaptive
estimation with well-understood stability properties. The EKF is optimal for
gradual drift in $s$ but responds slowly to step changes: a sudden halving of
hashrate takes $O(1/q)$ cohorts to track, where $q$ must be set conservatively
to avoid noise amplification.

In practice, mining operations are switched on and off discretely, producing
step changes in $s$ rather than smooth drift. The BOCPD's changepoint mechanism
responds to these in $O(1)$ cohorts regardless of magnitude. The EKF also
requires Gaussian approximation of the geometric likelihood (poor near $R = 1$)
and matrix arithmetic (difficult to make bit-exact for consensus).

A hybrid approach (EKF within runs for gradual drift, changepoint detection
between runs) is theoretically appealing but adds complexity without clear
benefit given the operating timescales (~seconds between cohorts vs ~minutes
between significant hashrate events).

## Comparison with Other Approaches

P2Pool v2 (Kulpreet Singh, Bitcoin), SChernykh's P2Pool (Monero), and Ethereum
(pre-merge) all handle parallel work through *uncle inclusion*: a sequential
share chain is maintained, and stale parallel shares are retroactively
referenced and partially rewarded by the next canonical share. Hydrapool (256
Foundation) is a deployment wrapper over P2Pool v2, using the same library.

P2Pool v2 for Bitcoin uses ASERT difficulty adjustment targeting 10-second share
times with a 10-minute half-life. Uncle shares receive 90% of their difficulty
weight; the including share receives a 10% bonus per uncle. The reward mechanism
is PPLNS over a ~2-week share chain window, with atomic swaps for small miners
who cannot receive direct coinbase payouts. SChernykh's Monero P2Pool uses a
similar structure with a 20% uncle penalty (80% to the uncle miner, 20% to the
includer). Both maintain a linear share chain with uncle references.

Ethereum (pre-merge) implemented a simplified GHOST protocol where uncle blocks
within 7 generations received $(8-d)/8$ of block reward, with a maximum of 2
uncles per block. Nephew blocks (those including uncles) received a 1/32 bonus.

Braidpool takes a fundamentally different approach: since the sidechain is a
true DAG where each bead may have multiple parents, parallel beads are not
"uncles" requiring inclusion. They are first-class members of the DAG.
Consensus is established by *cohorts* (graph cuts), and every bead in every
cohort is fully rewarded. There is no winner/uncle distinction and no partial-
reward penalization.

On difficulty adjustment: P2Pool v2 uses ASERT targeting a fixed share time.
Ethereum's EIP-100 targeted a fixed block time corrected for uncle rate.
Braidpool targets the ratio $N_B/N_C \approx 2.42$, which self-calibrates to
the observed propagation delay without requiring a manually-tuned share time.

On share rate: P2Pool v2 targets 10-second shares, producing ~60 shares per
Bitcoin block. Braidpool targets sub-second bead times (~500ms), producing
~1000 beads per Bitcoin block. A similar DAG network, Kaspa, achieves 100ms
block times, which is not out of reach for Braidpool. At 100ms bead times,
Braidpool can pay ~100x more miners or ~100x smaller miners than P2Pool v2,
significantly reducing payout variance for small operations.

On centralization: Ethereum's steep uncle reward decay favored large pools with
better peering. Sapirshtein et al. (2016) showed this reduces the selfish
mining threshold below 25%. P2Pool v2's 10% uncle penalty creates milder
pressure. Braidpool's cohort mechanism avoids this: all beads in a cohort are
fully rewarded, so there is no differential incentive favoring better-connected
miners beyond the basic requirement of propagating beads within the network
latency $a$.
