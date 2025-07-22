import streamlit as st
import duckdb
from lxml import etree
import tempfile
import os
import pandas as pd
from io import BytesIO
import hashlib
from pathlib import Path

# Configuração da página
st.set_page_config(
    page_title="Analisador de XML - Notas Fiscais (Otimizado)",
    page_icon="📊",
    layout="wide",
)

# Tags ICMS para busca
icms_tags = ["ICMS00", "ICMS20"]

# Diretório para armazenar bancos de dados
DB_DIR = Path("./databases")
DB_DIR.mkdir(exist_ok=True)


def safe_float(value):
    """Converte valor para float de forma segura"""
    try:
        return float(value) if value else None
    except Exception:
        return None


def get_file_hash(uploaded_files):
    """Gera hash único baseado nos arquivos enviados"""
    hasher = hashlib.md5()
    for file in uploaded_files:
        hasher.update(file.name.encode())
        hasher.update(str(file.size).encode())
    return hasher.hexdigest()


def get_db_connection(session_id):
    """Retorna conexão DuckDB persistente"""
    db_path = DB_DIR / f"session_{session_id}.duckdb"
    return duckdb.connect(str(db_path))


def process_xml_files(uploaded_files, session_id):
    """Processa arquivos XML enviados e retorna conexão DuckDB com dados"""
    # Conecta ao DuckDB em arquivo
    con = get_db_connection(session_id)

    # Verifica se a tabela já existe e tem dados
    try:
        existing_count = con.execute("SELECT COUNT(*) FROM itens_completos").fetchone()[
            0
        ]
        if existing_count > 0:
            st.info(
                f"ℹ️ Dados já processados encontrados ({existing_count} registros). Usando dados existentes."
            )
            return con, existing_count
    except:
        # Tabela não existe ainda
        pass

    # Namespace para XML de notas fiscais
    ns = {"ns": "http://www.portalfiscal.inf.br/nfcom"}

    dados_completos = []
    total_files = len(uploaded_files)

    # Cria tabela no DuckDB
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS itens_completos (
            filename VARCHAR,
            codigo_filial VARCHAR,
            nNF INTEGER,
            dhEmi TIMESTAMP,
            nItem VARCHAR,
            CFOP VARCHAR,
            cClass VARCHAR,
            vProd DOUBLE,
            vBC DOUBLE,
            vDesc DOUBLE,
            vOutro DOUBLE,
            vNF DOUBLE,
            pis_cst VARCHAR,
            cofins_cst VARCHAR,
            pis_vbc DOUBLE,
            cofins_vbc DOUBLE,
            vicms DOUBLE,
            ICMS_CODE VARCHAR,
            indicador_devolucao INTEGER,
            ind_sem_cst VARCHAR
        )
        """
    )

    # Barra de progresso
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Processa cada arquivo enviado
    for file_idx, uploaded_file in enumerate(uploaded_files):
        file_name = uploaded_file.name
        status_text.text(
            f"Processando arquivo {file_idx + 1}/{total_files}: {file_name}"
        )

        # Salva arquivo temporariamente
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name

        try:
            # Parse do XML
            tree = etree.parse(tmp_file_path)

            # Processa em lotes para melhor performance
            batch_data = []

            # Procura por cada nota fiscal
            for fatura in tree.findall("Fatura", namespaces=ns):
                # Extrai dados fiscais da nota (ide)
                nf_com_vivo = fatura.find("NFComVivo", namespaces=ns)
                ide = nf_com_vivo.find(".//ns:ide", namespaces=ns)
                if ide is None:
                    continue

                nNF_elem = ide.find("ns:nNF", namespaces=ns)
                dhEmi_elem = ide.find("ns:dhEmi", namespaces=ns)
                codigo_filial = tree.findtext(".//sirius/codigo_filial", namespaces=ns)

                if nNF_elem is None or dhEmi_elem is None:
                    continue

                try:
                    nNF = int(nNF_elem.text)
                    dhEmi = dhEmi_elem.text
                except Exception:
                    continue

                total_info = fatura.find(".//ns:total", namespaces=ns)
                vNF = (
                    total_info.findtext("ns:vNF", namespaces=ns)
                    if total_info is not None
                    else None
                )

                # Para cada item <det> dentro dessa nota
                for det in fatura.findall(".//ns:det", namespaces=ns):
                    nItem = det.get("nItem")
                    prod = det.find("ns:prod", namespaces=ns)
                    if prod is None:
                        continue

                    cfop = prod.findtext("ns:CFOP", namespaces=ns, default="0000")
                    cClass_text = prod.findtext("ns:cClass", namespaces=ns)
                    cClass = cClass_text[:3] if cClass_text else None

                    vProd = prod.findtext("ns:vProd", namespaces=ns)
                    vBC = prod.findtext("ns:vBC", namespaces=ns)
                    vDesc = prod.findtext("ns:vDesc", namespaces=ns)
                    vOutro = prod.findtext("ns:vOutro", namespaces=ns)

                    tag_imposto = det.find("ns:imposto", namespaces=ns)
                    if tag_imposto is None:
                        continue

                    indicador_devolucao = tag_imposto.findtext(
                        "ns:indDevolucao", namespaces=ns, default="0"
                    )
                    ind_sem_cst = tag_imposto.findtext(
                        "ns:indSemCST", namespaces=ns, default="-"
                    )

                    pis_cst = tag_imposto.find("ns:PIS/ns:CST", namespaces=ns)
                    cofins_cst = tag_imposto.find("ns:COFINS/ns:CST", namespaces=ns)

                    pis_vbc = tag_imposto.find("ns:PIS/ns:vBC", namespaces=ns)
                    cofins_vbc = tag_imposto.find("ns:COFINS/ns:vBC", namespaces=ns)

                    vicms = "0"
                    ICMS_CODE = "-"
                    for tag in icms_tags:
                        caminho = f".//ns:{tag}/ns:vICMS"
                        vICMS_element = tag_imposto.find(caminho, namespaces=ns)
                        if vICMS_element is not None:
                            vicms = vICMS_element.text
                            ICMS_CODE = tag.split("ICMS")[1]
                            break

                    # Converte valores para float
                    vProd = safe_float(vProd)
                    vBC = safe_float(vBC)
                    vDesc = safe_float(vDesc)
                    vOutro = safe_float(vOutro)
                    vNF = safe_float(vNF)
                    pis_vbc = safe_float(pis_vbc.text if pis_vbc is not None else None)
                    cofins_vbc = safe_float(
                        cofins_vbc.text if cofins_vbc is not None else None
                    )
                    vicms = safe_float(vicms if vicms is not None else None)

                    batch_data.append(
                        (
                            file_name,
                            codigo_filial,
                            nNF,
                            dhEmi,
                            nItem,
                            cfop,
                            cClass,
                            vProd,
                            vBC,
                            vDesc,
                            vOutro,
                            vNF,
                            pis_cst.text if pis_cst is not None else None,
                            cofins_cst.text if cofins_cst is not None else None,
                            pis_vbc,
                            cofins_vbc,
                            vicms,
                            ICMS_CODE,
                            indicador_devolucao,
                            ind_sem_cst,
                        )
                    )

                    # Insere em lotes de 1000 registros para melhor performance
                    if len(batch_data) >= 1000:
                        con.executemany(
                            """
                            INSERT INTO itens_completos (
                                filename, codigo_filial, nNF, dhEmi, nItem,
                                CFOP, cClass, vProd, vBC,
                                vDesc, vOutro, vNF, pis_cst,
                                cofins_cst, pis_vbc, cofins_vbc, vicms,
                                ICMS_CODE, indicador_devolucao, ind_sem_cst
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            batch_data,
                        )
                        dados_completos.extend(batch_data)
                        batch_data = []

            # Insere dados restantes do lote
            if batch_data:
                con.executemany(
                    """
                    INSERT INTO itens_completos (
                        filename, codigo_filial, nNF, dhEmi, nItem,
                        CFOP, cClass, vProd, vBC,
                        vDesc, vOutro, vNF, pis_cst,
                        cofins_cst, pis_vbc, cofins_vbc, vicms,
                        ICMS_CODE, indicador_devolucao, ind_sem_cst
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch_data,
                )
                dados_completos.extend(batch_data)

        finally:
            # Remove arquivo temporário
            os.unlink(tmp_file_path)

        # Atualiza barra de progresso
        progress_bar.progress((file_idx + 1) / total_files)

    # Limpa elementos de progresso
    progress_bar.empty()
    status_text.empty()

    # Cria índices para melhor performance das consultas
    try:
        con.execute("CREATE INDEX IF NOT EXISTS idx_nNF ON itens_completos(nNF)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_dhEmi ON itens_completos(dhEmi)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_CFOP ON itens_completos(CFOP)")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_filename ON itens_completos(filename)"
        )
    except:
        pass  # Índices podem já existir

    return con, len(dados_completos)


def main():
    st.title("📊 Analisador de XML - Notas Fiscais (Otimizado)")
    st.markdown("---")

    # Sidebar para upload e configurações
    with st.sidebar:
        st.header("📁 Upload de Arquivos")
        uploaded_files = st.file_uploader(
            "Selecione os arquivos XML",
            type=["xml"],
            accept_multiple_files=True,
            help="Selecione um ou mais arquivos XML de notas fiscais",
        )

        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)} arquivo(s) selecionado(s)")

            # Mostra informações dos arquivos
            total_size = sum(file.size for file in uploaded_files)
            st.write(f"**Tamanho total:** {total_size / (1024*1024):.1f} MB")

            with st.expander("📋 Lista de arquivos"):
                for file in uploaded_files:
                    size_mb = file.size / (1024 * 1024)
                    st.write(f"• {file.name} ({size_mb:.1f} MB)")

        st.markdown("---")

        # Opções de limpeza
        st.header("🧹 Gerenciamento")
        if st.button("🗑️ Limpar Dados", help="Remove todos os dados processados"):
            if "session_id" in st.session_state:
                try:
                    db_path = (
                        DB_DIR / f"session_{st.session_state['session_id']}.duckdb"
                    )
                    if db_path.exists():
                        db_path.unlink()
                    st.success("✅ Dados limpos com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erro ao limpar dados: {str(e)}")

    # Área principal
    if uploaded_files:
        # Gera ID da sessão baseado nos arquivos
        session_id = get_file_hash(uploaded_files)
        st.session_state["session_id"] = session_id

        with st.spinner("Processando arquivos XML..."):
            try:
                con, total_records = process_xml_files(uploaded_files, session_id)

                if total_records > 0:
                    st.success(
                        f"✅ Processamento concluído! {total_records} registros disponíveis."
                    )

                    # Armazena conexão no session state
                    st.session_state["db_connection"] = con
                    st.session_state["total_records"] = total_records

                    # Mostra informações básicas
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📄 Arquivos", len(uploaded_files))
                    with col2:
                        st.metric("📊 Registros", total_records)
                    with col3:
                        # Conta notas únicas
                        unique_notes = con.execute(
                            "SELECT COUNT(DISTINCT nNF) FROM itens_completos"
                        ).fetchone()[0]
                        st.metric("🧾 Notas Únicas", unique_notes)
                    with col4:
                        # Valor total
                        total_value = con.execute(
                            "SELECT SUM(vProd) FROM itens_completos WHERE vProd IS NOT NULL"
                        ).fetchone()[0]
                        if total_value:
                            st.metric("💰 Valor Total", f"R$ {total_value:,.2f}")

                else:
                    st.warning("⚠️ Nenhum registro foi encontrado nos arquivos XML.")

            except Exception as e:
                st.error(f"❌ Erro ao processar arquivos: {str(e)}")

    # Área de consultas SQL
    if "db_connection" in st.session_state:
        st.markdown("---")
        st.header("🔍 Consultas SQL")

        # Exemplos de consultas otimizadas
        with st.expander("📋 Exemplos de Consultas Otimizadas"):
            st.code(
                """
-- Listar primeiros registros (com LIMIT para performance)
SELECT * FROM itens_completos LIMIT 100;

-- Resumo por nota fiscal (otimizado)
SELECT 
    nNF,
    COUNT(*) as qtd_itens,
    MIN(dhEmi) as data_emissao, 
    SUM(vProd) as valor_total
FROM itens_completos
GROUP BY nNF
ORDER BY valor_total DESC
LIMIT 50;

-- Top 10 maiores valores por item
SELECT 
    filename, nNF, nItem, vProd, CFOP
FROM itens_completos 
WHERE vProd IS NOT NULL
ORDER BY vProd DESC 
LIMIT 10;

-- Análise por CFOP (com filtro de data)
SELECT 
    CFOP,
    COUNT(*) as quantidade,
    SUM(vProd) as valor_total,
    AVG(vProd) as valor_medio
FROM itens_completos 
WHERE dhEmi >= '2024-01-01'
GROUP BY CFOP 
ORDER BY valor_total DESC
LIMIT 20;

-- Análise temporal (por mês)
SELECT 
    date_trunc('month', dhEmi) as mes,
    COUNT(*) as qtd_itens,
    SUM(vProd) as valor_total
FROM itens_completos 
GROUP BY mes 
ORDER BY mes DESC;
            """
            )

        # Campo para consulta personalizada
        query = st.text_area(
            "Digite sua consulta SQL:",
            height=150,
            placeholder="SELECT * FROM itens_completos LIMIT 100;",
            help="Digite uma consulta SQL válida. Use LIMIT para consultas grandes!",
        )

        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            execute_query = st.button("🚀 Executar", type="primary")
        with col2:
            limit_results = st.checkbox("Limitar a 1000 linhas", value=True)

        if execute_query and query.strip():
            try:
                with st.spinner("Executando consulta..."):
                    # Adiciona LIMIT se solicitado e não presente na query
                    final_query = query.strip()
                    if limit_results and "LIMIT" not in final_query.upper():
                        final_query += " LIMIT 1000"

                    result = st.session_state["db_connection"].execute(final_query).df()

                    if not result.empty:
                        st.success(
                            f"✅ Consulta executada! {len(result)} linha(s) retornada(s)."
                        )

                        # Mostra resultado em tabela
                        st.dataframe(result, use_container_width=True, hide_index=True)

                        # Opção para download
                        csv = result.to_csv(index=False)
                        st.download_button(
                            label="📥 Baixar Resultado (CSV)",
                            data=csv,
                            file_name="resultado_consulta.csv",
                            mime="text/csv",
                        )

                    else:
                        st.info("ℹ️ A consulta não retornou resultados.")

            except Exception as e:
                st.error(f"❌ Erro na consulta SQL: {str(e)}")

        elif execute_query and not query.strip():
            st.warning("⚠️ Por favor, digite uma consulta SQL.")

    else:
        st.info("👆 Faça upload de arquivos XML para começar a análise.")

        # Mostra informações sobre otimização
        st.markdown("---")
        st.header("⚡ Otimizações Implementadas")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                """
            **🗄️ Banco de Dados Persistente**
            - DuckDB em arquivo (não em memória)
            - Dados persistem entre sessões
            - Melhor performance para arquivos grandes
            """
            )

        with col2:
            st.markdown(
                """
            **🚀 Performance Melhorada**
            - Processamento em lotes
            - Índices automáticos
            - Barra de progresso
            - Reutilização de dados processados
            """
            )


if __name__ == "__main__":
    main()
