from __future__ import annotations

from typing import List, Tuple, Optional
import pyodbc

TemplateRow = Tuple[str, str, str, str, str, str, str, str]


def insert_template_rows(cursor: pyodbc.Cursor, rows: List[TemplateRow]) -> int:
    sql = """
    INSERT INTO dbo.EgswRecipeImportTemplate
      (col1, col2, col3, col4, col5, col6, col7, col8)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.fast_executemany = True
    cursor.executemany(sql, rows)
    return len(rows)


def exec_usp_recipeimport_xls_and_get_idmain(
    cursor: pyodbc.Cursor,
    file_name: str,
    code_site: int = 1,
    code_user: int = 1,
    site_language: int = 1,
) -> int:
    """Call dbo.usp_RecipeImport_xls and return the *first* resultset's IDMain.

    Important:
    - dbo.usp_RecipeImport_xls (as provided) **does not** have an OUTPUT parameter.
      It returns IDMain via the first SELECT in the procedure.
    - The procedure returns multiple result sets (IDMain, InvalidCount, InvalidReport),
      so we must advance through remaining sets to avoid "connection is busy" errors
      before executing the next statement.
    """

    sql = """
    EXEC dbo.usp_RecipeImport_xls
      @FileName = ?,
      @CompareByName = 1,
      @CompareIngredientByName = 1,
      @CodeSite = ?,
      @CodeSetPrice = 1,
      @CodeTrans = 1,
      @CodeUser = ?,
      @SiteLanguage = ?,
      @OverwriteNumber = 1,
      @OverwriteName = 1,
      @OverwriteSubname = 1,
      @OverwriteYield = 1,
      @OverwriteSubrecipe = 1,
      @OverwriteSource = 1,
      @OverwriteCategory = 1,
      @OverwriteRemark = 1,
      @OverwriteDescription = 1,
      @OverwriteNotes = 1,
      @OverwriteAdditionalNotes = 1,
      @OverwriteIngredient = 1,
      @OverwriteProcedure = 1,
      @OverwriteKeyword = 1,
      @OverwriteAllergen = 1;
    """

    cursor.execute(sql, (file_name, code_site, code_user, site_language))

    # Result set 1: SELECT @IDMain as IDMain
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise RuntimeError("usp_RecipeImport_xls did not return IDMain in the first result set")

    id_main = int(row[0])

    # Consume/skip remaining result sets so we can execute the next statement safely.
    while cursor.nextset():
        # We don't need these results in the API right now.
        pass

    return id_main


def exec_usp_importrecipe(cursor: pyodbc.Cursor, id_main: int) -> None:
    cursor.execute("EXEC dbo.usp_RecipeImport_xls_ImportRecipe @IDMain = ?", (id_main,))


def import_nooko_rows_to_cmweb(
    conn: pyodbc.Connection,
    rows: List[TemplateRow],
    file_name: str,
    code_site: int = 1,
    code_user: int = 1,
    site_language: int = 1,
) -> int:
    """
    Full pipeline:
      1) insert staging rows into EgswRecipeImportTemplate
      2) create batch via usp_RecipeImport_xls -> IdMain
      3) import via usp_RecipeImport_xls_ImportRecipe(IdMain)
    """
    cursor = conn.cursor()
    try:
        insert_template_rows(cursor, rows)
        id_main = exec_usp_recipeimport_xls_and_get_idmain(
            cursor,
            file_name=file_name,
            code_site=code_site,
            code_user=code_user,
            site_language=site_language,
        )
        exec_usp_importrecipe(cursor, id_main)
        conn.commit()
        return id_main
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
