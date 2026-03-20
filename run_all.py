"""
run_all.py — Orquestador del pipeline financiero

Uso:
  py run_all.py           -> solo genera el briefing diario
  py run_all.py --weekly  -> FRED + precios historicos + briefing diario
"""

import subprocess, sys, os, time

BASE = os.path.dirname(os.path.abspath(__file__))


def run_script(script_name, label):
    path = os.path.join(BASE, script_name)
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    t0 = time.time()
    result = subprocess.run([sys.executable, path])
    elapsed = time.time() - t0
    status = "OK" if result.returncode == 0 else f"ERROR (code {result.returncode})"
    print(f"\n  [{status}] {script_name} — {elapsed:.1f}s")
    return result.returncode == 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline financiero — The Global Compounder")
    parser.add_argument("--weekly", action="store_true",
                        help="Incluir descarga semanal (FRED + precios historicos)")
    args = parser.parse_args()

    all_ok = True
    if args.weekly:
        all_ok &= run_script("download_macro_fred.py", "1/3  FRED — datos macro")
        all_ok &= run_script("download_data.py",       "2/3  Precios — historial y snapshot")
        step = "3/3"
    else:
        step = "1/1"

    all_ok &= run_script("daily_pipeline.py", f"{step}  Briefing diario")

    print(f"\n{'='*55}")
    print(f"  {'Listo.' if all_ok else 'Completado con errores — revisa los logs.'}")
    print(f"{'='*55}\n")
