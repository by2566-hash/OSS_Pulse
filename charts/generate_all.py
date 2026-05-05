"""Generate all presentation charts for OSS Pulse."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import numpy as np
import os

# Style
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.size': 13,
    'axes.titlesize': 16,
    'axes.titleweight': 'bold',
    'axes.labelsize': 13,
    'figure.facecolor': 'white',
    'axes.facecolor': '#FAFAFA',
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': '--',
})
BLUE = '#1B4F72'
ORANGE = '#E67E22'
RED = '#C0392B'
GREEN = '#27AE60'
GRAY = '#7F8C8D'
PURPLE = '#8E44AD'
COLORS = [BLUE, ORANGE, GREEN, RED, PURPLE]

DATA = '/Users/lizhechen/Downloads/BDAD/OSS_Pulse/data'
OUT = '/Users/lizhechen/Downloads/BDAD/OSS_Pulse/charts'

# ═══════════════════════════════════════════════════════════════════════════════
# Chart 1: Stars ≠ Health — Bubble Chart
# ═══════════════════════════════════════════════════════════════════════════════

hs = pd.read_csv(f'{DATA}/health_score.csv')
hs = hs.dropna(subset=['gh_stars_2025', 'pypi_downloads_2025'])

fig, ax = plt.subplots(figsize=(12, 7))
sizes = np.clip(hs['hf_model_count'] / 500, 20, 800)

scatter = ax.scatter(
    hs['pypi_downloads_2025'], hs['gh_stars_2025'],
    s=sizes, alpha=0.6, c=hs['health_score'],
    cmap='RdYlGn', edgecolors='white', linewidth=0.8,
    vmin=6, vmax=15
)
plt.colorbar(scatter, label='Health Score', shrink=0.8)

# Annotate key points
highlights = {
    'huggingface/transformers': (12, -20),
    'ollama/ollama': (-70, -25),
    'ukplab/sentence-transformers': (12, -20),
    'scikit-learn/scikit-learn': (12, 12),
    'pytorch/pytorch': (-30, 18),
}
for _, row in hs.iterrows():
    if row['repo_name'] in highlights:
        name = row['repo_name'].split('/')[1]
        ox, oy = highlights[row['repo_name']]
        ax.annotate(name, (row['pypi_downloads_2025'], row['gh_stars_2025']),
                    xytext=(ox, oy), textcoords='offset points',
                    fontsize=10, fontweight='bold',
                    arrowprops=dict(arrowstyle='->', color=GRAY, lw=1.2))

ax.set_xscale('log')
ax.set_xlabel('PyPI Downloads (2025, log scale)')
ax.set_ylabel('GitHub Stars (2025)')
ax.set_title('Stars ≠ Health: PyPI Downloads vs GitHub Stars\n(bubble size = HF model count, color = health score)')
fig.tight_layout()
fig.savefig(f'{OUT}/01_stars_vs_health.png', dpi=200)
plt.close()
print('[OK] 01_stars_vs_health.png')


# ═══════════════════════════════════════════════════════════════════════════════
# Chart 2: Contributor Risk — Horizontal Bar
# ═══════════════════════════════════════════════════════════════════════════════

ch = pd.read_csv(f'{DATA}/contributor_health.csv')
ch = ch.drop_duplicates(subset='repo_name')

# Select interesting repos
picks = [
    'pytorch/pytorch', 'huggingface/transformers', 'vllm-project/vllm',
    'langchain-ai/langchain', 'ollama/ollama', 'open-webui/open-webui',
    'n8n-io/n8n', 'deepseek-ai/deepseek-r1',
    'cline/cline', 'anthropics/claude-code',
    'sindresorhus/awesome', 'tensorflow/tensorflow',
]
subset = ch[ch['repo_name'].isin(picks)].copy()
subset = subset.sort_values('top1_push_ratio', ascending=True)
subset['short_name'] = subset['repo_name'].apply(lambda x: x.split('/')[1])

fig, ax = plt.subplots(figsize=(10, 7))
colors_bar = []
for _, row in subset.iterrows():
    if pd.isna(row['top1_push_ratio']):
        colors_bar.append(GRAY)
    elif row['top1_push_ratio'] < 0.3:
        colors_bar.append(GREEN)
    elif row['top1_push_ratio'] < 0.7:
        colors_bar.append(ORANGE)
    else:
        colors_bar.append(RED)

bars = ax.barh(subset['short_name'], subset['top1_push_ratio'].fillna(0),
               color=colors_bar, edgecolor='white', height=0.7)

# Add PR contributor count
for i, (_, row) in enumerate(subset.iterrows()):
    pr_c = row.get('pr_contributors', 0)
    if pd.notna(pr_c) and pr_c > 0:
        ax.text(min(row['top1_push_ratio'] + 0.02, 0.95) if pd.notna(row['top1_push_ratio']) else 0.02,
                i, f'PR: {int(pr_c)}', va='center', fontsize=9, color=BLUE)

ax.set_xlabel('Top-1 Push Ratio (higher = more concentrated)')
ax.set_title('Contributor Concentration: Who Really Pushes Code?\n(green = healthy, red = single-point risk)')
ax.set_xlim(0, 1.2)
ax.axvline(x=0.5, color=GRAY, linestyle=':', alpha=0.5)
fig.tight_layout()
fig.savefig(f'{OUT}/02_contributor_risk.png', dpi=200)
plt.close()
print('[OK] 02_contributor_risk.png')


# ═══════════════════════════════════════════════════════════════════════════════
# Chart 3: 5-Year Ecosystem Shift — Dual Line
# ═══════════════════════════════════════════════════════════════════════════════

sm = pd.read_csv(f'{DATA}/era_comparison/summary_metrics.csv')
eras = sm['era'].tolist()
x = range(len(eras))

fig, ax1 = plt.subplots(figsize=(12, 6.5))

# Developers (left axis)
line1 = ax1.plot(x, sm['distinct_actors'] / 1e6, 'o-', color=BLUE,
                 linewidth=2.5, markersize=8, label='Developers (M)')
ax1.set_ylabel('Developers (millions)', color=BLUE, fontsize=13)
ax1.tick_params(axis='y', labelcolor=BLUE)
ax1.set_ylim(6.5, 11.5)

# Events (right axis)
ax2 = ax1.twinx()
line2 = ax2.plot(x, sm['total_events'] / 1e6, 's--', color=ORANGE,
                 linewidth=2.5, markersize=8, label='Events (M)')
ax2.set_ylabel('Total Events (millions)', color=ORANGE, fontsize=13)
ax2.tick_params(axis='y', labelcolor=ORANGE)
ax2.set_ylim(180, 420)

ax1.set_xticks(x)
ax1.set_xticklabels(eras, fontsize=12)
ax1.set_title('5-Year OSS Ecosystem: More Developers, Fewer Events\n(2022–2026 Q1)',
              pad=15)

# Combined legend
lines = line1 + line2
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc='upper left', fontsize=11)

# Annotate 2026 drop — bottom right to avoid title
ax2.annotate('Events -29%\nDevelopers +3%', xy=(4, sm['total_events'].iloc[4]/1e6),
             xytext=(3.0, 220),
             fontsize=11, color=RED, fontweight='bold',
             arrowprops=dict(arrowstyle='->', color=RED, lw=1.5))

fig.tight_layout()
fig.savefig(f'{OUT}/03_era_shift.png', dpi=200)
plt.close()
print('[OK] 03_era_shift.png')


# ═══════════════════════════════════════════════════════════════════════════════
# Chart 4: Weekend Gap Disappeared — Bar Chart
# ═══════════════════════════════════════════════════════════════════════════════

ww = pd.read_csv(f'{DATA}/dev_rhythm/weekday_weekend.csv')
eras_list = ww['era'].unique()

weekend_pct = []
for era in sorted(eras_list):
    sub = ww[ww['era'] == era]
    total = sub['events'].sum()
    wkend = sub[sub['is_weekend'] == True]['events'].sum()
    weekend_pct.append(wkend / total * 100)

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(range(len(sorted(eras_list))), weekend_pct,
              color=[BLUE, BLUE, BLUE, BLUE, ORANGE],
              edgecolor='white', width=0.6)

# Natural baseline
ax.axhline(y=28.9, color=RED, linestyle='--', linewidth=2, alpha=0.7, label='Natural ratio (28.9%)')

# Labels on bars
for i, (bar, pct) in enumerate(zip(bars, weekend_pct)):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
            f'{pct:.1f}%', ha='center', fontsize=12, fontweight='bold')

ax.set_xticks(range(len(sorted(eras_list))))
ax.set_xticklabels(sorted(eras_list))
ax.set_ylabel('Weekend Event Share (%)')
ax.set_title('The Weekend Gap Disappeared\n(weekend share approaching natural 28.9% baseline)')
ax.set_ylim(20, 33)
ax.legend(fontsize=11)
fig.tight_layout()
fig.savefig(f'{OUT}/04_weekend_gap.png', dpi=200)
plt.close()
print('[OK] 04_weekend_gap.png')


# ═══════════════════════════════════════════════════════════════════════════════
# Chart 5: Agent Two Phases — Dual Axis
# ═══════════════════════════════════════════════════════════════════════════════

pa = pd.read_csv(f'{DATA}/dev_rhythm/push_per_actor.csv')
eras_pa = pa['era'].tolist()
x = range(len(eras_pa))

fig, ax1 = plt.subplots(figsize=(12, 6.5))

# Push actors (left, bar)
bars = ax1.bar(x, pa['total_push_actors'] / 1e6, color=BLUE, alpha=0.7,
               width=0.5, label='Push Actors (M)', edgecolor='white')
ax1.set_ylabel('Push Actors (millions)', color=BLUE, fontsize=13)
ax1.tick_params(axis='y', labelcolor=BLUE)
ax1.set_ylim(0, 10.5)

# >50 pushes/day accounts (right, line)
ax2 = ax1.twinx()
ax2.plot(x, pa['actors_gt_50_per_day'], 'o-', color=RED,
         linewidth=2.5, markersize=10, label='>50 pushes/day accounts')
ax2.set_ylabel('High-frequency Accounts', color=RED, fontsize=13)
ax2.tick_params(axis='y', labelcolor=RED)
ax2.set_ylim(2000, 16000)

ax1.set_xticks(x)
ax1.set_xticklabels(eras_pa, fontsize=12)
ax1.set_title('Two Phases of the Agent Era\n(more people push, but bots peaked in 2025)',
              pad=15)

# Annotate phases — positioned to avoid title overlap
ax2.annotate('2025: Explosion\n12,743 high-freq', xy=(3, 12743),
             xytext=(1.3, 14500), fontsize=10, color=RED, fontweight='bold',
             arrowprops=dict(arrowstyle='->', color=RED, lw=1.5))
ax2.annotate('2026: Normalized\n6,575', xy=(4, 6575),
             xytext=(3.8, 3500), fontsize=10, color=GRAY,
             arrowprops=dict(arrowstyle='->', color=GRAY, lw=1.2))

# Combined legend
h1, l1 = ax1.get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
ax1.legend(h1+h2, l1+l2, loc='upper left', fontsize=11)

fig.tight_layout()
fig.savefig(f'{OUT}/05_agent_phases.png', dpi=200)
plt.close()
print('[OK] 05_agent_phases.png')


# ═══════════════════════════════════════════════════════════════════════════════
# Chart 6: Push Weekend % by Event Type — Grouped Bar
# ═══════════════════════════════════════════════════════════════════════════════

wbt = pd.read_csv(f'{DATA}/dev_rhythm/weekday_weekend_by_type.csv')

event_types = ['PushEvent', 'PullRequestEvent']
eras_sorted = sorted(wbt['era'].unique())

fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

for idx, et in enumerate(event_types):
    ax = axes[idx]
    pcts = []
    for era in eras_sorted:
        sub = wbt[(wbt['era'] == era) & (wbt['event_type'] == et)]
        total = sub['events'].sum()
        wkend = sub[sub['is_weekend'] == True]['events'].sum()
        pcts.append(wkend / total * 100 if total > 0 else 0)

    color = BLUE if et == 'PushEvent' else PURPLE
    bars = ax.bar(range(len(eras_sorted)), pcts,
                  color=[color]*4 + [ORANGE], edgecolor='white', width=0.6)
    ax.axhline(y=28.9, color=RED, linestyle='--', linewidth=1.5, alpha=0.6)

    for i, (bar, pct) in enumerate(zip(bars, pcts)):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f'{pct:.1f}%', ha='center', fontsize=10, fontweight='bold')

    ax.set_xticks(range(len(eras_sorted)))
    ax.set_xticklabels(eras_sorted, fontsize=10)
    ax.set_title(et.replace('Event', ''), fontsize=14, fontweight='bold')
    ax.set_ylim(15, 35)

axes[0].set_ylabel('Weekend Share (%)')
fig.suptitle('Weekend Activity by Event Type\n(red line = 28.9% natural baseline)', fontsize=15, fontweight='bold')
fig.tight_layout()
fig.savefig(f'{OUT}/06_weekend_by_type.png', dpi=200)
plt.close()
print('[OK] 06_weekend_by_type.png')


# ═══════════════════════════════════════════════════════════════════════════════
# Chart 7: Era Event Type Distribution — Stacked Bar
# ═══════════════════════════════════════════════════════════════════════════════

dq = pd.read_csv(f'{DATA}/data_quality_check.csv')

fig, ax = plt.subplots(figsize=(11, 6))
eras_dq = dq['era'].tolist()
x = range(len(eras_dq))
width = 0.6

types = ['push_events', 'pr_events', 'watch_events', 'issue_events', 'fork_events']
labels = ['Push', 'PR', 'Watch (Star)', 'Issues', 'Fork']
colors_stack = [BLUE, PURPLE, ORANGE, GREEN, GRAY]

bottom = np.zeros(len(eras_dq))
for typ, label, color in zip(types, labels, colors_stack):
    vals = dq[typ].values / 1e6
    ax.bar(x, vals, width, bottom=bottom, label=label, color=color, edgecolor='white')
    bottom += vals

ax.set_xticks(x)
ax.set_xticklabels(eras_dq)
ax.set_ylabel('Events (millions)')
ax.set_title('Event Type Distribution Across 5 Eras\n(note WatchEvent -63% and ForkEvent -67% in 2026)')
ax.legend(loc='upper left')
fig.tight_layout()
fig.savefig(f'{OUT}/07_event_distribution.png', dpi=200)
plt.close()
print('[OK] 07_event_distribution.png')


print(f'\n[DONE] All charts saved to {OUT}/')
