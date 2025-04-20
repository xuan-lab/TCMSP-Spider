import pandas as pd
import os
import argparse
import sys
import logging # << NEW: For logging >>
from pathlib import Path # << NEW: For easier path handling >>
from datetime import datetime # << NEW: For timestamps in log >>
# --- Fuzzy Matching Dependency ---
try:
    from thefuzz import process, fuzz # << NEW: For fuzzy matching >>
except ImportError:
    print("错误：缺少 'thefuzz' 库（以及可选的 'python-Levenshtein'）。")
    print("请先通过 'pip install thefuzz[speedup]' 安装它以获得最佳性能。")
    sys.exit(1)


# --- Dependencies ---
# This script requires 'openpyxl' to read/write .xlsx files: pip install openpyxl
# This script now requires 'thefuzz' for fuzzy matching: pip install thefuzz[speedup]

# --- Configuration ---
# Input/Output filenames are handled by command-line arguments

# Define the column name to merge on.
merge_column = 'molecule_name'
# Define the PubChem ID column name (ensure this matches the actual column name in table1)
pubchem_id_column = 'PubChem_id'
# --- Fuzzy Matching Configuration --- <<< NEW >>>
# Score threshold (0-100) for considering names a match. Adjust as needed.
# Higher values mean stricter matching. 90 is a reasonable starting point.
FUZZY_MATCH_THRESHOLD = 90
# Column name for the matched name from file 1 in the merged output
MATCHED_NAME_COLUMN = 'matched_molecule_name_file1'
# Column name for the similarity score in the merged output
MATCH_SCORE_COLUMN = 'match_similarity_score'


# --- Setup Logging --- <<< NEW >>>
script_dir = Path(__file__).parent # Get the directory where the script is located
log_file_path = script_dir / 'merge_log.log' # Log file in the same directory
output_dir_path = script_dir / 'merge_file' # Output directory

# Create output directory if it doesn't exist
try:
    output_dir_path.mkdir(parents=True, exist_ok=True)
except OSError as e:
    print(f"错误：无法创建输出目录 '{output_dir_path}': {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'), # Log to file
        logging.StreamHandler(sys.stdout) # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# --- Function to load data ---
def load_data(filename):
    """
    Loads data from an Excel file (.xlsx) into a pandas DataFrame.
    Args:
        filename (str): The path to the Excel file.
    Returns:
        pandas.DataFrame: The loaded DataFrame, or None if the file is not found, empty, or an error occurs.
    """
    if not os.path.exists(filename):
        logger.error(f"文件 '{filename}' 未找到。请确保文件存在于正确的目录中。")
        return None
    try:
        # Use read_excel for .xlsx files
        df = pd.read_excel(filename, engine='openpyxl')
        if df.empty:
            logger.warning(f"文件 '{filename}' 为空。")
        else:
             logger.info(f"成功加载文件 '{filename}'，包含 {len(df)} 行。")
        return df
    except ImportError:
        logger.error("错误：缺少 'openpyxl' 库。请先通过 'pip install openpyxl' 安装它。")
        return None
    except Exception as e:
        logger.error(f"加载文件 '{filename}' 时出错: {e}")
        return None

# --- Function for Fuzzy Matching --- <<< NEW >>>
def find_best_match(name_to_match, choices, scorer=fuzz.token_sort_ratio, score_cutoff=FUZZY_MATCH_THRESHOLD):
    """
    Finds the best fuzzy match for a name within a list of choices.
    Args:
        name_to_match (str): The name to find a match for.
        choices (list): A list of potential matching names.
        scorer: The scoring function from thefuzz (default: token_sort_ratio).
        score_cutoff (int): The minimum score (0-100) to consider a match.
    Returns:
        tuple: (best_match, score) or (None, 0) if no match above cutoff.
    """
    if not name_to_match or not isinstance(name_to_match, str) or not choices:
        return None, 0
    # extractOne returns (choice, score)
    best_match_result = process.extractOne(name_to_match, choices, scorer=scorer, score_cutoff=score_cutoff)
    if best_match_result:
        return best_match_result[0], best_match_result[1]
    else:
        return None, 0

# --- Main script ---
if __name__ == "__main__":
    logger.info("--- 合并脚本开始 ---")
    logger.info(f"日志文件: {log_file_path}")
    logger.info(f"输出目录: {output_dir_path}")

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description='使用模糊匹配合并两个 Excel 表格脚本')
    parser.add_argument('input_file1', type=str, help='第一个输入的 Excel 文件路径 (.xlsx)')
    parser.add_argument('input_file2', type=str, help='第二个输入的 Excel 文件路径 (.xlsx)')
    parser.add_argument('--threshold', type=int, default=FUZZY_MATCH_THRESHOLD,
                        help=f'模糊匹配相似度阈值 (0-100，默认: {FUZZY_MATCH_THRESHOLD})')

    # Check if any arguments were passed, provide help if not
    if len(sys.argv) < 3: # Need script name + 2 arguments
        parser.print_help(sys.stderr)
        logger.error("缺少输入文件参数。")
        sys.exit(1)
    args = parser.parse_args()
    file1 = args.input_file1
    file2 = args.input_file2
    fuzzy_threshold = args.threshold
    logger.info(f"使用的模糊匹配阈值: {fuzzy_threshold}")


    # --- Derive Output Filenames ---
    base_name1, ext1 = os.path.splitext(os.path.basename(file1)) # Use basename to avoid including input path
    if ext1.lower() != '.xlsx':
         logger.error(f"第一个输入文件 '{file1}' 必须是 .xlsx 格式。")
         sys.exit(1)
    # Check second file extension too
    base_name2, ext2 = os.path.splitext(os.path.basename(file2))
    if ext2.lower() != '.xlsx':
         logger.error(f"第二个输入文件 '{file2}' 必须是 .xlsx 格式。")
         sys.exit(1)

    # Generate output names based on the first input file, placed in the output directory
    output_file_merged = output_dir_path / f"{base_name1}_merged{ext1}"
    output_file_pubchem = output_dir_path / f"{base_name1}_with_pubchem_id{ext1}"

    logger.info("重要提示：此脚本需要 'openpyxl' 和 'thefuzz' 库。")
    logger.info("如果尚未安装，请运行: pip install openpyxl 'thefuzz[speedup]'")
    logger.info(f"第一个输入文件: {file1}")
    logger.info(f"第二个输入文件: {file2}")
    logger.info(f"合并输出文件: {output_file_merged}")
    logger.info(f"筛选后输出文件: {output_file_pubchem}")

    logger.info(f"正在加载第一个表格: {file1}")
    df1 = load_data(file1)

    logger.info(f"正在加载第二个表格: {file2}")
    df2 = load_data(file2)

    # Proceed only if both DataFrames were loaded successfully
    if df1 is not None and df2 is not None:
        # --- Data Cleaning Step ---
        if merge_column in df1.columns and merge_column in df2.columns:
            logger.info(f"正在清理 '{merge_column}' 列中的前后空格...")
            # Ensure the merge columns are string type before cleaning
            df1[merge_column] = df1[merge_column].astype(str).str.strip()
            df2[merge_column] = df2[merge_column].astype(str).str.strip()
            # Optional: Convert to lowercase for case-insensitive matching (before fuzzy matching)
            # df1[merge_column] = df1[merge_column].str.lower()
            # df2[merge_column] = df2[merge_column].str.lower()
            logger.info("空格清理完成。")
        else:
            if merge_column not in df1.columns:
                logger.error(f"合并列 '{merge_column}' 在文件 '{file1}' 中未找到。无法进行清理和合并。")
                logger.error(f"'{file1}' 中的可用列: {list(df1.columns)}")
            if merge_column not in df2.columns:
                logger.error(f"合并列 '{merge_column}' 在文件 '{file2}' 中未找到。无法进行清理和合并。")
                logger.error(f"'{file2}' 中的可用列: {list(df2.columns)}")
            logger.error("由于合并列缺失，无法继续执行合并操作。")
            sys.exit(1)

        # --- Fuzzy Matching Step --- <<< NEW >>>
        logger.info(f"正在为 '{file2}' 中的 '{merge_column}' 列查找模糊匹配项 (阈值={fuzzy_threshold})...")
        # Get unique, non-empty names from df1 to use as choices
        choices_df1 = df1[df1[merge_column].notna() & (df1[merge_column] != '')][merge_column].unique().tolist()
        if not choices_df1:
             logger.error(f"文件 '{file1}' 的 '{merge_column}' 列中没有找到有效的名称用于匹配。")
             sys.exit(1)

        logger.info(f"从 '{file1}' 中提取了 {len(choices_df1)} 个唯一名称用于匹配。")

        # Apply the fuzzy matching function to each name in df2
        match_results = df2[merge_column].apply(
            lambda name: find_best_match(name, choices_df1, score_cutoff=fuzzy_threshold)
        )

        # Add the match results as new columns in df2
        df2[MATCHED_NAME_COLUMN] = [result[0] for result in match_results]
        df2[MATCH_SCORE_COLUMN] = [result[1] for result in match_results]

        # Filter df2 to keep only rows where a match was found above the threshold
        matched_df2 = df2[df2[MATCHED_NAME_COLUMN].notna()].copy()
        unmatched_count = len(df2) - len(matched_df2)
        logger.info(f"模糊匹配完成。在 '{file2}' 的 {len(df2)} 行中，有 {len(matched_df2)} 行找到了匹配项（得分 >= {fuzzy_threshold}）。")
        if unmatched_count > 0:
            logger.warning(f"{unmatched_count} 行来自 '{file2}' 的名称未能找到足够相似的匹配项，将被排除在合并之外。")
            # Optional: Save unmatched rows for review
            # unmatched_df2 = df2[df2[MATCHED_NAME_COLUMN].isna()]
            # unmatched_file = output_dir_path / f"{base_name2}_unmatched.xlsx"
            # try:
            #     unmatched_df2.to_excel(unmatched_file, index=False, engine='openpyxl')
            #     logger.info(f"未匹配的行已保存到 '{unmatched_file}'")
            # except Exception as e:
            #     logger.error(f"保存未匹配文件 '{unmatched_file}' 时出错: {e}")


        # --- Merging Step (using the matched names) ---
        if not matched_df2.empty:
            logger.info(f"正在以 '{merge_column}' (来自 {os.path.basename(file1)}) 和 '{MATCHED_NAME_COLUMN}' (来自 {os.path.basename(file2)}) 为基准合并表格...")
            try:
                # Merge df1 with the filtered/matched df2
                # Use left_on for df1's original name, right_on for the matched name in df2
                merged_df = pd.merge(
                    df1,
                    matched_df2,
                    left_on=merge_column,
                    right_on=MATCHED_NAME_COLUMN,
                    how='inner', # Inner join ensures only matches are kept
                    suffixes=('_file1', '_file2') # Add suffixes if columns other than merge keys overlap
                )

                # Optional: Clean up columns - remove the extra matched name column from df2 if desired
                # merged_df = merged_df.drop(columns=[MATCHED_NAME_COLUMN])
                # Optional: Rename the original merge column from file2 if needed
                # merged_df = merged_df.rename(columns={f'{merge_column}_file2': 'original_molecule_name_file2'})


                logger.info(f"合并完成。")
                logger.info(f"原始表格1行数 (清理后): {len(df1)}")
                logger.info(f"原始表格2行数 (找到匹配项): {len(matched_df2)}")
                logger.info(f"合并后表格总行数: {len(merged_df)}")
                logger.info(f"合并后表格列数: {len(merged_df.columns)}")
                logger.info("\n合并后表格的前5行:")
                # Log head() output cleanly
                logger.info("\n" + merged_df.head().to_string())


                # --- Save the full merged DataFrame ---
                try:
                    merged_df.to_excel(output_file_merged, index=False, engine='openpyxl')
                    logger.info(f"完整的合并表格已成功保存到 '{output_file_merged}'")
                except ImportError:
                     logger.error(f"错误：缺少 'openpyxl' 库。无法保存 Excel 文件 '{output_file_merged}'。请先通过 'pip install openpyxl' 安装它。")
                except Exception as e:
                    logger.error(f"保存文件 '{output_file_merged}' 时出错: {e}")

                # --- Filter and save rows with PubChem ID ---
                # Ensure the PubChem ID column exists (check potential suffixed names if necessary)
                pubchem_col_to_check = pubchem_id_column
                if pubchem_col_to_check not in merged_df.columns:
                    # If columns were suffixed, check the suffixed version from file1
                    suffixed_pubchem_col = f"{pubchem_id_column}_file1"
                    if suffixed_pubchem_col in merged_df.columns:
                        pubchem_col_to_check = suffixed_pubchem_col
                        logger.warning(f"原始 PubChem 列 '{pubchem_id_column}' 未找到，将使用 suffixed 列 '{pubchem_col_to_check}' 进行筛选。")
                    else:
                         logger.warning(f"列 '{pubchem_id_column}' (或带有后缀的版本) 在合并后的表格中未找到。无法筛选并保存 PubChem ID 子集。")
                         logger.warning(f"合并后表格的列: {list(merged_df.columns)}")
                         pubchem_col_to_check = None # Indicate column not found


                if pubchem_col_to_check:
                    logger.info(f"正在筛选 '{pubchem_col_to_check}' 列有值的行...")
                    pubchem_filtered_df = merged_df[
                        merged_df[pubchem_col_to_check].notna() & \
                        (merged_df[pubchem_col_to_check].astype(str).str.strip() != '')
                    ].copy()

                    logger.info(f"筛选完成。找到 {len(pubchem_filtered_df)} 行包含有效的 '{pubchem_col_to_check}'。")

                    if not pubchem_filtered_df.empty:
                        try:
                            pubchem_filtered_df.to_excel(output_file_pubchem, index=False, engine='openpyxl')
                            logger.info(f"包含 PubChem ID 的行已成功保存到 '{output_file_pubchem}'")
                        except ImportError:
                            logger.error(f"错误：缺少 'openpyxl' 库。无法保存 Excel 文件 '{output_file_pubchem}'。请先通过 'pip install openpyxl' 安装它。")
                        except Exception as e:
                            logger.error(f"保存文件 '{output_file_pubchem}' 时出错: {e}")
                    else:
                        logger.info(f"未找到包含有效 '{pubchem_col_to_check}' 的行，因此未创建文件 '{output_file_pubchem}'。")

            except Exception as e:
                logger.error(f"合并或处理表格时出错: {e}", exc_info=True) # Log traceback
        else:
             logger.warning(f"在文件 '{file2}' 中没有找到与 '{file1}' 匹配的行（阈值={fuzzy_threshold}），无法执行合并。")

    else:
        logger.error("由于一个或多个文件加载失败，无法执行后续操作。")

    logger.info("--- 合并脚本结束 ---")
