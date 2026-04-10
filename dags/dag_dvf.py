from __future__ import annotations

import logging
import os
import tempfile
import unicodedata
import zipfile
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import requests
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DVF_DATASET_API_URL = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres/"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"
TARGET_DEPARTEMENTS = ["75", "92"]

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : telechargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():
    def current_period() -> tuple[int, int]:
        now = datetime.now()
        return now.year, now.month

    def to_float(value, default=None):
        if value is None:
            return default
        return float(value)

    def to_int(value, default=None):
        if value is None:
            return default
        return int(value)

    def resolve_dvf_resource_url() -> tuple[str, int]:
        """
        Recupere dynamiquement une URL DVF valide depuis l'API data.gouv.
        Priorite: annee courante, sinon derniere annee disponible.
        """
        response = requests.get(DVF_DATASET_API_URL, timeout=20)
        response.raise_for_status()
        payload = response.json()
        resources = payload.get("resources", [])
        if not resources:
            raise AirflowException("Aucune ressource DVF disponible dans l'API data.gouv.")

        candidates: list[tuple[int, str]] = []
        for resource in resources:
            title = str(resource.get("title", ""))
            url = str(resource.get("url", ""))
            if not url:
                continue
            digits = "".join(ch for ch in title if ch.isdigit())
            year = int(digits[-4:]) if len(digits) >= 4 else 0
            if year >= 2000:
                candidates.append((year, url))

        if not candidates:
            raise AirflowException("Impossible d'extraire une URL DVF annuelle valide.")

        current_year = datetime.now().year
        by_year = {year: url for year, url in candidates}
        if current_year in by_year:
            return by_year[current_year], current_year

        latest_year = max(by_year.keys())
        return by_year[latest_year], latest_year

    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        """
        Verifie la disponibilite des sources de donnees et de l'infrastructure.
        Retourne un dictionnaire avec le statut de l'API DVF et de HDFS.
        """
        statuts = {}

        try:
            resp_dvf = requests.get(DVF_DATASET_API_URL, timeout=10)
            statuts["dvf_api"] = resp_dvf.ok
        except requests.RequestException:
            statuts["dvf_api"] = False

        try:
            hdfs_url = f"{WEBHDFS_BASE_URL}/?op=LISTSTATUS&user.name={WEBHDFS_USER}"
            resp_hdfs = requests.get(hdfs_url, timeout=10, allow_redirects=True)
            statuts["hdfs"] = resp_hdfs.ok
        except requests.RequestException:
            statuts["hdfs"] = False

        logger.info("Statut API DVF: %s", statuts["dvf_api"])
        logger.info("Statut HDFS: %s", statuts["hdfs"])

        if not statuts["dvf_api"] or not statuts["hdfs"]:
            raise AirflowException(f"Source indisponible: {statuts}")

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        """
        Telecharge le fichier CSV DVF depuis data.gouv.fr en streaming.
        Retourne le chemin local du fichier.
        """
        if not statuts.get("dvf_api"):
            raise AirflowException("API DVF indisponible selon verifier_sources.")

        resource_url, annee = resolve_dvf_resource_url()
        local_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}.csv")
        local_zip_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}.zip")

        logger.info("Telechargement DVF depuis %s", resource_url)
        downloaded = 0
        next_log_threshold = 50 * 1024 * 1024

        with requests.get(resource_url, stream=True, timeout=300) as response:
            response.raise_for_status()
            with open(local_zip_path, "wb") as file_out:
                for chunk in response.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    file_out.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= next_log_threshold:
                        logger.info("Telecharge: %.2f Mo", downloaded / (1024 * 1024))
                        next_log_threshold += 50 * 1024 * 1024

        with zipfile.ZipFile(local_zip_path, "r") as zf:
            txt_files = [name for name in zf.namelist() if name.lower().endswith(".txt")]
            if not txt_files:
                raise AirflowException(f"Aucun .txt trouve dans l'archive DVF: {local_zip_path}")
            txt_name = txt_files[0]
            with zf.open(txt_name, "r") as src, open(local_path, "wb") as dst:
                dst.write(src.read())

        if os.path.exists(local_zip_path):
            os.remove(local_zip_path)

        file_size = os.path.getsize(local_path)
        if file_size < 1000:
            raise AirflowException(
                f"Fichier telecharge trop petit ({file_size} octets): {local_path}"
            )

        logger.info("Fichier DVF telecharge: %s (%.2f Mo)", local_path, file_size / (1024 * 1024))
        return local_path

    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        """
        Uploade le fichier CSV local vers HDFS (zone raw / data lake) avec
        partitionnement Hive-style: annee=YYYY/dept=XX.
        Retourne le chemin HDFS du fichier pour le dept 75 (ou le premier dispo).
        """
        annee, _ = current_period()
        tmp_dir = tempfile.gettempdir()
        dept_local_paths: dict[str, str] = {}
        dept_has_data: dict[str, bool] = {}

        for dept in TARGET_DEPARTEMENTS:
            dept_local_paths[dept] = os.path.join(tmp_dir, f"dvf_{annee}_{dept}.csv")
            dept_has_data[dept] = False
            if os.path.exists(dept_local_paths[dept]):
                os.remove(dept_local_paths[dept])

        for chunk in pd.read_csv(local_path, sep="|", dtype=str, chunksize=200_000, low_memory=False):
            if "Code departement" not in chunk.columns:
                raise AirflowException("Colonne 'Code departement' absente du fichier DVF.")
            for dept in TARGET_DEPARTEMENTS:
                dept_chunk = chunk[chunk["Code departement"] == dept]
                if dept_chunk.empty:
                    continue
                dept_chunk.to_csv(
                    dept_local_paths[dept],
                    sep="|",
                    index=False,
                    mode="a",
                    header=not dept_has_data[dept],
                )
                dept_has_data[dept] = True

        hdfs_uploaded_paths: dict[str, str] = {}

        def webhdfs_upload(src_path: str, dest_path: str) -> None:
            init_url = (
                f"{WEBHDFS_BASE_URL}{dest_path}"
                f"?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true"
            )
            init_resp = requests.put(init_url, allow_redirects=False, timeout=30)
            if init_resp.status_code not in (307, 201):
                raise AirflowException(
                    f"Echec initiation upload HDFS ({init_resp.status_code}): {init_resp.text}"
                )

            redirect_url = init_resp.headers.get("Location")
            if not redirect_url and init_resp.status_code == 307:
                raise AirflowException("URL de redirection WebHDFS manquante.")

            if redirect_url:
                with open(src_path, "rb") as file_in:
                    upload_resp = requests.put(
                        redirect_url,
                        data=file_in,
                        headers={"Content-Type": "application/octet-stream"},
                        allow_redirects=False,
                        timeout=300,
                    )
                if upload_resp.status_code != 201:
                    raise AirflowException(
                        f"Echec upload DataNode ({upload_resp.status_code}): {upload_resp.text}"
                    )

        for dept in TARGET_DEPARTEMENTS:
            if not dept_has_data[dept]:
                logger.warning("Aucune ligne pour dept=%s, partition ignoree.", dept)
                continue
            partition_dir = f"{HDFS_RAW_PATH}/annee={annee}/dept={dept}"
            mkdir_url = f"{WEBHDFS_BASE_URL}{partition_dir}?op=MKDIRS&user.name={WEBHDFS_USER}"
            mkdir_resp = requests.put(mkdir_url, timeout=30)
            mkdir_resp.raise_for_status()
            if mkdir_resp.json().get("boolean") is not True:
                raise AirflowException(f"Echec MKDIRS pour {partition_dir}: {mkdir_resp.text}")

            hdfs_file_path = f"{partition_dir}/dvf_{annee}_{dept}.csv"
            webhdfs_upload(dept_local_paths[dept], hdfs_file_path)
            hdfs_uploaded_paths[dept] = hdfs_file_path
            logger.info("Fichier partitionne upload vers HDFS: %s", hdfs_file_path)

        if os.path.exists(local_path):
            os.remove(local_path)
        for path in dept_local_paths.values():
            if os.path.exists(path):
                os.remove(path)

        if not hdfs_uploaded_paths:
            raise AirflowException("Aucune partition HDFS n'a ete uploadee.")

        return hdfs_uploaded_paths.get("75", next(iter(hdfs_uploaded_paths.values())))

    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        """
        Lit le CSV depuis HDFS, applique les filtres metier et calcule les agregats.
        Retourne les agregats par arrondissement et les stats globales.
        """

        def normalize_col(col: str) -> str:
            col = col.strip().lower()
            col = "".join(
                c for c in unicodedata.normalize("NFD", col) if unicodedata.category(c) != "Mn"
            )
            return col.replace(" ", "_")

        open_url = f"{WEBHDFS_BASE_URL}{hdfs_path}?op=OPEN&user.name={WEBHDFS_USER}"
        response = requests.get(open_url, allow_redirects=True, timeout=300)
        response.raise_for_status()

        raw_bytes = response.content
        try:
            df = pd.read_csv(BytesIO(raw_bytes), sep="|", low_memory=False)
        except Exception:
            df = pd.read_csv(BytesIO(raw_bytes), low_memory=False)

        df.columns = [normalize_col(c) for c in df.columns]

        required_cols = [
            "date_mutation",
            "nature_mutation",
            "valeur_fonciere",
            "code_postal",
            "type_local",
            "surface_reelle_bati",
            "nombre_pieces_principales",
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise AirflowException(f"Colonnes manquantes dans le CSV DVF: {missing_cols}")

        initial_rows = len(df)
        df["valeur_fonciere"] = pd.to_numeric(
            df["valeur_fonciere"].astype(str).str.replace(",", ".", regex=False), errors="coerce"
        )
        df["surface_reelle_bati"] = pd.to_numeric(
            df["surface_reelle_bati"].astype(str).str.replace(",", ".", regex=False), errors="coerce"
        )
        df["code_postal"] = df["code_postal"].astype(str).str.extract(r"(\d{5})", expand=False)

        df = df[df["type_local"] == "Appartement"]
        df = df[df["nature_mutation"] == "Vente"]
        df = df[df["surface_reelle_bati"].between(9, 500, inclusive="both")]
        df = df[df["valeur_fonciere"] > 10000]
        df = df[df["code_postal"].isin([f"750{str(i).zfill(2)}" for i in range(1, 21)] + ["75116"])]
        df = df[df["surface_reelle_bati"] > 0]

        logger.info("Filtrage DVF: %s lignes -> %s lignes", initial_rows, len(df))

        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        def extract_arrdt(code_postal: str) -> int:
            if code_postal == "75116":
                return 16
            return int(code_postal[-2:])

        df["arrondissement"] = df["code_postal"].apply(extract_arrdt)

        grouped = (
            df.groupby(["code_postal", "arrondissement"], as_index=False)
            .agg(
                prix_m2_moyen=("prix_m2", "mean"),
                prix_m2_median=("prix_m2", "median"),
                prix_m2_min=("prix_m2", "min"),
                prix_m2_max=("prix_m2", "max"),
                nb_transactions=("prix_m2", "count"),
                surface_moyenne=("surface_reelle_bati", "mean"),
            )
            .sort_values(["arrondissement", "code_postal"])
        )

        annee, mois = current_period()
        grouped["annee"] = annee
        grouped["mois"] = mois

        agregats = grouped.round(2).to_dict(orient="records")

        avg_by_arrdt = df.groupby("arrondissement")["prix_m2"].mean()
        stats_globales = {
            "annee": annee,
            "mois": mois,
            "nb_transactions_total": int(len(df)),
            "prix_m2_median_paris": float(round(df["prix_m2"].median(), 2)) if len(df) else None,
            "prix_m2_moyen_paris": float(round(df["prix_m2"].mean(), 2)) if len(df) else None,
            "arrdt_plus_cher": int(avg_by_arrdt.idxmax()) if len(avg_by_arrdt) else None,
            "arrdt_moins_cher": int(avg_by_arrdt.idxmin()) if len(avg_by_arrdt) else None,
            "surface_mediane": float(round(df["surface_reelle_bati"].median(), 2)) if len(df) else None,
        }

        return {"agregats": agregats, "stats_globales": stats_globales}

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        """
        Insere les donnees agregees dans PostgreSQL (zone curated) en UPSERT.
        Retourne le nombre de lignes traitees.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})

        upsert_arrdt_query = """
        INSERT INTO prix_m2_arrondissement
        (code_postal, arrondissement, annee, mois,
         prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
         nb_transactions, surface_moyenne, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
            prix_m2_moyen = EXCLUDED.prix_m2_moyen,
            prix_m2_median = EXCLUDED.prix_m2_median,
            prix_m2_min = EXCLUDED.prix_m2_min,
            prix_m2_max = EXCLUDED.prix_m2_max,
            nb_transactions = EXCLUDED.nb_transactions,
            surface_moyenne = EXCLUDED.surface_moyenne,
            updated_at = NOW();
        """

        upsert_stats_query = """
        INSERT INTO stats_marche
        (annee, mois, nb_transactions_total, prix_m2_median_paris, prix_m2_moyen_paris,
         arrdt_plus_cher, arrdt_moins_cher, surface_mediane, date_calcul)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (annee, mois) DO UPDATE SET
            nb_transactions_total = EXCLUDED.nb_transactions_total,
            prix_m2_median_paris = EXCLUDED.prix_m2_median_paris,
            prix_m2_moyen_paris = EXCLUDED.prix_m2_moyen_paris,
            arrdt_plus_cher = EXCLUDED.arrdt_plus_cher,
            arrdt_moins_cher = EXCLUDED.arrdt_moins_cher,
            surface_mediane = EXCLUDED.surface_mediane,
            date_calcul = NOW();
        """

        lignes_traitees = 0
        for row in agregats:
            params = (
                str(row.get("code_postal")),
                to_int(row.get("arrondissement"), 0),
                to_int(row.get("annee"), 0),
                to_int(row.get("mois"), 0),
                to_float(row.get("prix_m2_moyen")),
                to_float(row.get("prix_m2_median")),
                to_float(row.get("prix_m2_min")),
                to_float(row.get("prix_m2_max")),
                to_int(row.get("nb_transactions"), 0),
                to_float(row.get("surface_moyenne")),
            )
            hook.run(upsert_arrdt_query, parameters=params)
            lignes_traitees += 1

        if stats_globales:
            params_stats = (
                to_int(stats_globales.get("annee"), 0),
                to_int(stats_globales.get("mois"), 0),
                to_int(stats_globales.get("nb_transactions_total"), 0),
                to_float(stats_globales.get("prix_m2_median_paris")),
                to_float(stats_globales.get("prix_m2_moyen_paris")),
                to_int(stats_globales.get("arrdt_plus_cher")),
                to_int(stats_globales.get("arrdt_moins_cher")),
                to_float(stats_globales.get("surface_mediane")),
            )
            hook.run(upsert_stats_query, parameters=params_stats)
            lignes_traitees += 1

        logger.info("PostgreSQL: %s ligne(s) inseree(s)/mise(s) a jour.", lignes_traitees)
        return lignes_traitees

    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        """
        Genere un rapport des arrondissements classes par prix median au m2.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        annee, mois = current_period()

        query = """
        SELECT
            arrondissement,
            prix_m2_median,
            prix_m2_moyen,
            nb_transactions,
            surface_moyenne
        FROM prix_m2_arrondissement
        WHERE annee = %s AND mois = %s
        ORDER BY prix_m2_median DESC
        LIMIT 20;
        """
        records = hook.get_records(query, parameters=(annee, mois))

        lines = [
            f"Rapport DVF ({annee}-{str(mois).zfill(2)}) - {nb_inseres} ligne(s) inseree(s)/maj",
            "Arrondissement | Median (EUR/m2) | Moyen (EUR/m2) | Transactions | Surface moyenne",
            "---------------|-----------------|----------------|-------------|----------------",
        ]
        for arrdt, median, moyen, nb_tx, surf_moy in records:
            lines.append(
                f"{arrdt:>13} | {float(median or 0):>15.2f} | {float(moyen or 0):>14.2f} | "
                f"{int(nb_tx or 0):>11} | {float(surf_moy or 0):>14.2f}"
            )

        rapport = "\n".join(lines)
        logger.info("\n%s", rapport)
        return rapport

    @task(task_id="analyser_tendances")
    def analyser_tendances(rapport: str) -> str:
        """
        Calcule l'evolution mois sur mois du prix median par arrondissement
        et stocke un resume dans stats_marche.
        """
        _ = rapport
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        annee, mois = current_period()
        if mois == 1:
            prev_annee, prev_mois = annee - 1, 12
        else:
            prev_annee, prev_mois = annee, mois - 1

        trend_query = """
        SELECT
            a.arrondissement,
            a.prix_m2_median AS prix_m2_median_courant,
            b.prix_m2_median AS prix_m2_median_precedent,
            ROUND(
                ((a.prix_m2_median - b.prix_m2_median) / NULLIF(b.prix_m2_median, 0)) * 100,
                2
            ) AS variation_pct
        FROM prix_m2_arrondissement a
        JOIN prix_m2_arrondissement b
          ON a.arrondissement = b.arrondissement
        WHERE a.annee = %s
          AND a.mois = %s
          AND b.annee = %s
          AND b.mois = %s
        ORDER BY variation_pct DESC;
        """
        hook.run(
            """
            ALTER TABLE stats_marche
            ADD COLUMN IF NOT EXISTS arrdt_plus_hausse INTEGER,
            ADD COLUMN IF NOT EXISTS arrdt_plus_baisse INTEGER,
            ADD COLUMN IF NOT EXISTS variation_max_pct NUMERIC(10, 2),
            ADD COLUMN IF NOT EXISTS variation_min_pct NUMERIC(10, 2),
            ADD COLUMN IF NOT EXISTS tendance_moyenne_pct NUMERIC(10, 2);
            """
        )

        records = hook.get_records(trend_query, parameters=(annee, mois, prev_annee, prev_mois))

        if not records:
            hook.run(
                """
                UPDATE stats_marche
                SET arrdt_plus_hausse = NULL,
                    arrdt_plus_baisse = NULL,
                    variation_max_pct = NULL,
                    variation_min_pct = NULL,
                    tendance_moyenne_pct = NULL
                WHERE annee = %s AND mois = %s;
                """,
                parameters=(annee, mois),
            )
            msg = (
                f"Aucune tendance calculee pour {annee}-{str(mois).zfill(2)} "
                f"(pas de reference {prev_annee}-{str(prev_mois).zfill(2)})."
            )
            logger.info(msg)
            return msg

        lines = [
            f"Tendances DVF ({annee}-{str(mois).zfill(2)} vs {prev_annee}-{str(prev_mois).zfill(2)})",
            "Arrondissement | Prix prec. | Prix cour. | Variation %",
            "---------------|------------|------------|------------",
        ]
        for arrdt, prix_courant, prix_precedent, variation in records:
            lines.append(
                f"{int(arrdt):>13} | {float(prix_precedent or 0):>10.2f} | "
                f"{float(prix_courant or 0):>10.2f} | {float(variation or 0):>10.2f}"
            )
        trend_report = "\n".join(lines)
        logger.info("\n%s", trend_report)

        variations = [float(r[3]) for r in records if r[3] is not None]
        top_up = max(records, key=lambda r: float(r[3])) if variations else None
        top_down = min(records, key=lambda r: float(r[3])) if variations else None
        moyenne = round(sum(variations) / len(variations), 2) if variations else None

        hook.run(
            """
            UPDATE stats_marche
            SET arrdt_plus_hausse = %s,
                arrdt_plus_baisse = %s,
                variation_max_pct = %s,
                variation_min_pct = %s,
                tendance_moyenne_pct = %s
            WHERE annee = %s AND mois = %s;
            """,
            parameters=(
                int(top_up[0]) if top_up else None,
                int(top_down[0]) if top_down else None,
                float(top_up[3]) if top_up and top_up[3] is not None else None,
                float(top_down[3]) if top_down and top_down[3] is not None else None,
                moyenne,
                annee,
                mois,
            ),
        )
        return trend_report

    t_verif = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs = stocker_hdfs_raw(t_download)
    t_traiter = traiter_donnees(t_hdfs)
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_pg)
    t_tendances = analyser_tendances(t_rapport)
    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport, t_tendances)


pipeline_dvf()
