{% extends "base.html" %}

{% block title %}Estoque - Metrify{% endblock %}

{% block content %}
<div class="container-fluid py-4">

    <!-- Cabeçalho -->
    <div class="d-flex justify-content-between align-items-center mb-4">
        <div>
            <h1 class="h3 mb-1">Estoque e Reposição</h1>
            <p class="text-muted mb-0">
                Acompanhe o estoque atual, custo total e previsão de cobertura por produto.
            </p>
        </div>
        <div>
            <!-- Botão para abrir modal de ajuste -->
            <button class="btn btn-primary btn-sm" data-bs-toggle="modal" data-bs-target="#modalAjusteEstoque">
                <i class="bi bi-arrow-left-right me-1"></i>
                Novo ajuste de estoque
            </button>
        </div>
    </div>

    <!-- Cards de totais de estoque -->
    <div class="row g-3 mb-4">
        <!-- Total unidades -->
        <div class="col-12 col-md-6 col-xl-4">
            <div class="card shadow-sm border-0 h-100">
                <div class="card-body d-flex">
                    <div class="flex-grow-1">
                        <div class="text-muted text-uppercase small mb-1">
                            Unidades totais em estoque
                        </div>
                        <div class="h4 mb-0">
                            {{ total_unidades_estoque|int }}
                        </div>
                        <small class="text-muted">
                            Soma de todas as unidades disponíveis.
                        </small>
                    </div>
                    <div class="ms-3 d-flex align-items-center">
                        <div class="rounded-circle bg-primary bg-opacity-10 p-3">
                            <i class="bi bi-boxes text-primary fs-4"></i>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Custo total do estoque -->
        <div class="col-12 col-md-6 col-xl-4">
            <div class="card shadow-sm border-0 h-100">
                <div class="card-body d-flex">
                    <div class="flex-grow-1">
                        <div class="text-muted text-uppercase small mb-1">
                            Custo total do estoque
                        </div>
                        <div class="h4 mb-0">
                            R$ {{ '%.2f'|format(total_custo_estoque or 0) }}
                        </div>
                        <small class="text-muted">
                            Estoque atual multiplicado pelo custo unitário de cada produto.
                        </small>
                    </div>
                    <div class="ms-3 d-flex align-items-center">
                        <div class="rounded-circle bg-success bg-opacity-10 p-3">
                            <i class="bi bi-cash-stack text-success fs-4"></i>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Parâmetros de cálculo -->
        <div class="col-12 col-md-12 col-xl-4">
            <div class="card shadow-sm border-0 h-100">
                <div class="card-body d-flex">
                    <div class="flex-grow-1">
                        <div class="text-muted text-uppercase small mb-1">
                            Parâmetros de análise
                        </div>
                        <div class="mb-1">
                            <span class="badge bg-light text-dark me-2">
                                Janela: últimos {{ janela_dias }} dias
                            </span>
                            <span class="badge bg-light text-dark">
                                Estoque mínimo: {{ dias_minimos }} dias
                            </span>
                        </div>
                        <small class="text-muted d-block">
                            A média diária de vendas é calculada dentro da janela de {{ janela_dias }} dias.
                        </small>
                        <small class="text-muted">
                            Produtos com cobertura abaixo de {{ dias_minimos }} dias aparecem como
                            <strong>"Repôr"</strong>.
                        </small>
                    </div>
                    <div class="ms-3 d-flex align-items-center">
                        <div class="rounded-circle bg-info bg-opacity-10 p-3">
                            <i class="bi bi-info-circle text-info fs-4"></i>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Tabela de estoque -->
    <div class="card shadow-sm border-0">
        <div class="card-header bg-transparent border-0 d-flex justify-content-between align-items-center">
            <h5 class="mb-0">Detalhamento por produto</h5>
        </div>
        <div class="table-responsive">
            <table class="table align-middle mb-0">
                <thead class="table-light">
                    <tr>
                        <th>Produto</th>
                        <th>SKU</th>
                        <th class="text-end">Estoque atual (un)</th>
                        <th class="text-end">Custo unitário (R$)</th>
                        <th class="text-end">Custo em estoque (R$)</th>
                        <th class="text-end">Média diária (un)</th>
                        <th class="text-end">Média mensal (un)</th>
                        <th class="text-end">Cobertura (dias)</th>
                        <th class="text-center">Situação</th>
                    </tr>
                </thead>
                <tbody>
                    {% if produtos %}
                        {% for p in produtos %}
                            <tr class="{% if p.precisa_repor %}table-warning{% endif %}">
                                <td>{{ p.nome }}</td>
                                <td>{{ p.sku or '-' }}</td>
                                <td class="text-end">
                                    {{ '%.0f'|format(p.estoque_atual or 0) }}
                                </td>
                                <td class="text-end">
                                    R$ {{ '%.2f'|format(p.custo_unitario or 0) }}
                                </td>
                                <td class="text-end">
                                    R$ {{ '%.2f'|format(p.custo_estoque or 0) }}
                                </td>
                                <td class="text-end">
                                    {{ '%.0f'|format(p.media_diaria or 0) }}
                                </td>
                                <td class="text-end">
                                    {{ '%.0f'|format(p.media_mensal or 0) }}
                                </td>
                                <td class="text-end">
                                    {% if p.dias_cobertura is not none %}
                                        {{ '%.0f'|format(p.dias_cobertura) }}
                                    {% else %}
                                        -
                                    {% endif %}
                                </td>
                                <td class="text-center">
                                    {% if p.dias_cobertura is none %}
                                        <span class="badge bg-secondary-subtle text-secondary">
                                            Sem venda recente
                                        </span>
                                    {% elif p.precisa_repor %}
                                        <span class="badge bg-danger-subtle text-danger">
                                            Repôr
                                        </span>
                                    {% else %}
                                        <span class="badge bg-success-subtle text-success">
                                            OK
                                        </span>
                                    {% endif %}
                                </td>
                            </tr>
                        {% endfor %}
                    {% else %}
                        <tr>
                            <td colspan="9" class="text-center text-muted py-4">
                                Ainda não há produtos cadastrados ou movimentações de estoque.
                            </td>
                        </tr>
                    {% endif %}
                </tbody>
            </table>
        </div>
    </div>

</div>

<!-- Modal de ajuste de estoque -->
<div class="modal fade" id="modalAjusteEstoque" tabindex="-1" aria-labelledby="modalAjusteEstoqueLabel" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered">
        <div class="modal-content">
            <form method="POST" action="{{ url_for('ajuste_estoque') }}">
                <div class="modal-header">
                    <h5 class="modal-title" id="modalAjusteEstoqueLabel">Novo ajuste de estoque</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Fechar"></button>
                </div>
                <div class="modal-body">

                    <div class="mb-3">
                        <label class="form-label">Produto</label>
                        <select name="produto_id" class="form-select" required>
                            <option value="">Selecione...</option>
                            {% for p in produtos %}
                                <option value="{{ p.id }}">{{ p.nome }}{% if p.sku %} ({{ p.sku }}){% endif %}</option>
                            {% endfor %}
                        </select>
                    </div>

                    <div class="mb-3">
                        <label class="form-label">Tipo de ajuste</label>
                        <select name="tipo" class="form-select" required>
                            <option value="entrada">Entrada</option>
                            <option value="saida">Saída</option>
                        </select>
                        <small class="text-muted d-block">
                            Entrada aumenta o estoque, saída reduz.
                        </small>
                    </div>

                    <div class="mb-3">
                        <label class="form-label">Quantidade</label>
                        <input type="number" name="quantidade" class="form-control" min="1" required>
                    </div>

                    <div class="mb-3">
                        <label class="form-label">Novo custo unitário (opcional)</label>
                        <input type="number" step="0.01" name="custo_unitario" class="form-control">
                        <small class="text-muted">
                            Preencha apenas se quiser atualizar o custo unitário do produto.
                        </small>
                    </div>

                    <div class="mb-3">
                        <label class="form-label">Observação</label>
                        <textarea name="observacao" class="form-control" rows="2"
                                  placeholder="Ex.: Ajuste de inventário, entrada de nova compra, perda, etc."></textarea>
                    </div>

                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancelar</button>
                    <button type="submit" class="btn btn-primary">Salvar ajuste</button>
                </div>
            </form>
        </div>
    </div>
</div>
{% endblock %}
