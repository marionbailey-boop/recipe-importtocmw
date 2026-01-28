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
    """
    Calls the SP like the example and returns IdMain.
    Using DECLARE + SELECT avoids OUTPUT param handling issues in pyodbc.
    """
    sql = """
    DECLARE @IdMain INT;

    EXEC dbo.usp_RecipeImport_xls
      @FileName = ?,
      @CompareByName = 1,
      @CompareIngredientByName = 1,
      @CodeSite = 1,
      @CodeSetPrice = 1,
      @CodeTrans = 1,
      @CodeUser = 1,
      @SiteLanguage = 1,
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
      @OverwriteAllergen = 1,
      @IdMain = @IdMain OUTPUT;

    SELECT @IdMain AS IdMain;
    """
    cursor.execute(sql, (file_name, code_site, code_user, site_language))
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise RuntimeError("usp_RecipeImport_xls did not return IdMain")
    return int(row[0])


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
