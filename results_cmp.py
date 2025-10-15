import os
import sys

FILES = [
    "results_query_1.csv",
    "results_query_2_1.csv",
    "results_query_2_2.csv",
    "results_query_3.csv",
    "results_query_4.csv",
]

KAGGLE_REDUCED_DIR = ".kaggle_reduced_results"
KAGGLE_FULL_DIR = ".kaggle_results"
RESULTS_DIR = ".results"

def load_rows(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(line)
    return rows


def compare_sets(base_set, result_set):
    base_set = set(base_set)
    result_set = set(base_set)

    missing_in_results = base_set - result_set
    extra_in_results = result_set - base_set

    return sorted(missing_in_results), sorted(extra_in_results)

    return missing_in_base, extra_in_results


def get_args():
    verbose = False
    
    if len(sys.argv) < 2:
        print("Uso: python3 test_cmp.py <número_de_cliente> [--full]")
        return None, None, None, verbose
    
    clients_arg = sys.argv[1]
    try:
        client_size = int(clients_arg)
    except ValueError:
        print("El número de cliente debe ser un entero")
        return None, None, None, verbose
            
    base_dir = KAGGLE_REDUCED_DIR
    if len(sys.argv) > 2:
        if sys.argv[2] == "--full":
            base_dir = KAGGLE_FULL_DIR
        if sys.argv[2] == "--verbose":
            verbose = True
        if len(sys.argv) > 3:
            if sys.argv[3] == "--verbose":
                verbose = True
            if sys.argv[3] == "--full":
                base_dir = KAGGLE_FULL_DIR
    
    if not os.path.isdir(base_dir):
        print(f"No existe el directorio {base_dir}")
        return None, None, None, verbose

    if not os.path.isdir(RESULTS_DIR):
        print(f"No existe el directorio {RESULTS_DIR}")
        return None, None, None, verbose

    return clients_arg, base_dir, RESULTS_DIR, verbose

def results_compare(base_dir, result_dir, verbose=False):
    for file_name in FILES:
        base_src = os.path.join(base_dir, file_name)
        result_src = os.path.join(result_dir, file_name)

        if not os.path.exists(result_src):
            print(f"  {file_name} no existe en {result_dir}, se omite.\n")
            continue

        base_set = load_rows(base_src)
        result_set = load_rows(result_src)

        missing, extra = compare_sets(base_set, result_set)

        if not missing and not extra:
            print(f"✅ {file_name}: resultados iguales - {len(base_set)}/{len(result_set)} filas\n")
        else:
            print(f"❌ {file_name}: diferencias encontradas")
            if missing:
                print(f"  ▪ Faltan en resultados locales: {len(missing)} filas")
                if verbose:
                    for line in sorted(missing):
                        print("   -", line)
            if extra:
                print(f"  ▪ Líneas adicionales en resultados locales: {len(extra)} filas")
                if verbose:
                    for line in sorted(extra):
                        print("   +", line)
            print()

def main():

    clients_size, base_dir, result_dir, verbose = get_args()
    if clients_size is None:
        sys.exit(1)
    
    print(f"Comparando resultados con {base_dir} para {clients_size} clientes...\n")

    for client_id in range(1, int(clients_size) + 1):
        print(f"Comparando resultados del cliente {client_id}...")
        results_compare(base_dir, f"{result_dir}/client-{client_id}", verbose)
        print("--------------------------------------------------\n")

if __name__ == "__main__":
    main()
