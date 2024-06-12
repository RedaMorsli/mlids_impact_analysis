from dataset_utils import load_merged_dataset_by_attack_flow_based
import metadata as md


load_merged_dataset_by_attack_flow_based(md.ATTACK_DATA["WEB_1"]).to_csv(
    "./output/dataset/flow_based/web_dataset.csv"
)
