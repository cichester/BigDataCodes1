# evaluate_lore.py
import json

def evaluate_extraction(ground_truth_file: str, llm_results_file: str):
    """
    Calcola Precision, Recall e F1-Score per l'estrazione di entità.
    """
    with open(ground_truth_file, 'r', encoding='utf-8') as f:
        ground_truth_data = {item['champion_name']: item for item in json.load(f)}
    
    with open(llm_results_file, 'r', encoding='utf-8') as f:
        llm_results_data = {item['champion_name']: item for item in json.load(f)}

    # Inizializza i contatori
    total_tp = 0 # Veri Positivi
    total_fp = 0 # Falsi Positivi
    total_fn = 0 # Falsi Negativi

    # Itera sui campioni nel ground truth
    for champion, truth in ground_truth_data.items():
        if champion not in llm_results_data:
            continue

        predictions = llm_results_data[champion]
        
        # Uniamo alleati e rivali per una valutazione complessiva
        truth_entities = set(truth.get('allies', []) + truth.get('rivals', []))
        prediction_entities = set(predictions.get('allies', []) + predictions.get('rivals', []))

        # Calcola TP, FP, FN per questo campione
        tp = len(truth_entities.intersection(prediction_entities))
        fp = len(prediction_entities - truth_entities)
        fn = len(truth_entities - prediction_entities)
        
        total_tp += tp
        total_fp += fp
        total_fn += fn

    # Calcola le metriche finali
    precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
    recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    print("="*40)
    print("RISULTATI DELLA VALUTAZIONE (LORE EXTRACTION)")
    print("="*40)
    print(f"Veri Positivi (TP): {total_tp}")
    print(f"Falsi Positivi (FP): {total_fp}")
    print(f"Falsi Negativi (FN): {total_fn}")
    print("-"*40)
    print(f"Precision: {precision:.2%}")
    print(f"Recall:    {recall:.2%}")
    print(f"F1-Score:  {f1_score:.2%}")
    print("="*40)

if __name__ == '__main__':
    evaluate_extraction('ground_truth.json', 'lore_data.json')