# Mini-Challenge MSR 2026

## 🚀 Workflow de Execução

### ⚠️ IMPORTANTE: Cache Unificado de PRs

Para garantir **contagens consistentes** entre todos os scripts, execute primeiro:

```bash
python src/0_create_pr_cache.py
```

Este script busca todos os PRs dos repositórios **uma única vez** e armazena em cache.

📖 **Leia mais:** [CACHE_SOLUTION.md](CACHE_SOLUTION.md)

### Ordem de Execução

```bash
# 0. Criar cache unificado (EXECUTE PRIMEIRO!)
python src/0_create_pr_cache.py

# 1. Análise inicial e filtro de projetos
python src/1_get_all_projects.py

# 2. Buscar commits merged
python src/2_get_all_merged_commits.py

# 3. Análise de clones
python src/3_analyze_clones.py

# 4. Análise adicional
python src/4_analyze.py
```

## 📁 Estrutura de Resultados

- `01_results/` - Análise inicial de projetos e cache unificado
  - `unified_pr_cache.json` - Cache único de todos os PRs
  - `q3plus_projects_filtered.csv` - Projetos filtrados
  
- `02_results/` - Commits merged e detecção de agents
  - `projects_with_pr_sha.csv` - PRs com SHAs
  - `projects_summary_stats.csv` - Estatísticas por projeto

## 🔧 Solução do Problema de Contagens

**Problema:** Scripts retornavam contagens diferentes de PRs.

**Solução:** Cache unificado que busca dados uma vez via REST API e reutiliza em todos os scripts.

✅ **Resultado:** Contagens idênticas e consistentes!

## 🛠️ Configuração

1. Configure o token do GitHub no `.env`:
```bash
GITHUB_TOKEN=your_token_here
```

2. Instale dependências (se necessário)

3. Execute o workflow acima