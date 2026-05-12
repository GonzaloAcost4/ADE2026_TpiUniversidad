import pandas as pd
import transformacion as t


def main():
    print('Iniciando diagnóstico de exámenes...')
    df_ex = t.leer_tabla_staging('stg_examen')
    df_ins = t.leer_tabla_staging('stg_inscripcion')

    print(f"examenes totales (stg_examen): {len(df_ex)}")
    print(f"inscripciones totales (stg_inscripcion): {len(df_ins)}")

    # Detectar nombres reales de columnas
    def find_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    col_id_ins_ex = find_col(df_ex, ['id_inscripcion', 'id_inscripcion_raw', 'idInscripcion', 'id_insc'])
    col_id_ins_ins = find_col(df_ins, ['id_inscripcion', 'id_inscripcion_raw', 'idInscripcion', 'id_insc'])

    print('stg_examen columnas:', list(df_ex.columns)[:20])
    print('stg_inscripcion columnas:', list(df_ins.columns)[:20])

    if not col_id_ins_ex or not col_id_ins_ins:
        print('No se encontró columna id_inscripcion en una de las tablas. Revisar nombres de columnas.')
        return

    # Cuántos exámenes tienen id_inscripcion que existe
    existen_ins = df_ex[col_id_ins_ex].isin(df_ins[col_id_ins_ins]).sum()
    print(f"examenes con inscripcion conocida: {existen_ins}")

    # Normalizar ids desde columnas raw usando el mismo DataCleaner
    cleaner = t.DataCleaner()

    # detectar columnas raw para inscripcion
    col_id_ins_ins_raw = find_col(df_ins, ['id_inscripcion_raw', 'id_inscripcion', 'idInscripcion'])
    col_id_est_ins_raw = find_col(df_ins, ['id_estudiante_raw', 'id_estudiante', 'idEstudiante'])
    col_id_dic_ins_raw = find_col(df_ins, ['id_dictado_raw', 'id_dictado', 'idDictado'])

    if not col_id_ins_ins_raw or not col_id_est_ins_raw or not col_id_dic_ins_raw:
        print('No se encontraron columnas raw esperadas en stg_inscripcion; abortando diagnóstico')
        return

    ins_map = df_ins[[col_id_ins_ins_raw, col_id_est_ins_raw, col_id_dic_ins_raw]].drop_duplicates().copy()
    ins_map['id_inscripcion'] = ins_map[col_id_ins_ins_raw].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    ins_map['id_estudiante'] = ins_map[col_id_est_ins_raw].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    ins_map['id_dictado'] = ins_map[col_id_dic_ins_raw].apply(lambda x: cleaner.limpiar_numero(x, 'int'))

    # detectar columnas raw para examen
    col_id_ins_ex_raw = find_col(df_ex, ['id_inscripcion_raw', 'id_inscripcion', 'idInscripcion'])
    col_fecha_ex_raw = find_col(df_ex, ['fecha_raw', 'fecha', 'fecha_examen_raw'])
    col_nota_ex_raw = find_col(df_ex, ['nota_raw', 'nota'])
    col_result_ex_raw = find_col(df_ex, ['resultado_raw', 'resultado'])

    if not col_id_ins_ex_raw:
        print('No se encontró columna id_inscripcion en stg_examen; abortando diagnóstico')
        return

    df_ex_proc = df_ex.copy()
    df_ex_proc['id_inscripcion'] = df_ex_proc[col_id_ins_ex_raw].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    if col_fecha_ex_raw:
        df_ex_proc['fecha'] = df_ex_proc[col_fecha_ex_raw].apply(cleaner.limpiar_fecha)
    else:
        df_ex_proc['fecha'] = None
    if col_nota_ex_raw:
        df_ex_proc['nota'] = df_ex_proc[col_nota_ex_raw].apply(lambda x: cleaner.limpiar_numero(x, 'float'))
    else:
        df_ex_proc['nota'] = None
    if col_result_ex_raw:
        df_ex_proc['resultado'] = df_ex_proc[col_result_ex_raw].apply(cleaner.limpiar_string)
    else:
        df_ex_proc['resultado'] = None

    df = df_ex_proc.merge(ins_map[['id_inscripcion', 'id_estudiante', 'id_dictado']], on='id_inscripcion', how='left')

    completos = df[df['id_estudiante'].notna() & df['id_dictado'].notna()].copy()
    incompletos = df[~(df['id_estudiante'].notna() & df['id_dictado'].notna())].copy()

    print(f"examenes asociados a (estudiante,dictado): {len(completos)}")
    print(f"examenes sin asociacion (incompletos): {len(incompletos)}")

    grp = completos.groupby(['id_estudiante', 'id_dictado']).size()
    total_grupos = grp.size
    grupos_gt3 = (grp > 3).sum()
    max_size = int(grp.max()) if total_grupos>0 else 0
    mean_size = float(grp.mean()) if total_grupos>0 else 0

    print(f"grupos completos (estudiante,dictado): {total_grupos}")
    print(f"grupos con >3 intentos: {grupos_gt3}")
    print(f"max intentos en un grupo: {max_size} | promedio intentos por grupo: {mean_size:.2f}")

    # Calcular descartes replicando la lógica de consolidar_examenes
    def removed_by_rules(group):
        g = group.sort_values('fecha').copy()
        size = len(g)
        aprobado_mask = g['resultado'].fillna('').str.lower().str.contains('aprob') | (g['nota'].notna() & (g['nota'] >= 6))
        if aprobado_mask.any():
            first_ap = int(aprobado_mask.to_numpy().argmax())
            final_len = min(first_ap + 1, 3)
        else:
            final_len = min(size, 3)
        return size - final_len

    removed_sum = completos.groupby(['id_estudiante', 'id_dictado']).apply(removed_by_rules).sum()
    print(f"total eliminados por reglas (aprobado + truncamiento a 3): {int(removed_sum)}")

    # Desglose: cuántos se eliminan por aprobado (después del primer aprobado) y cuántos por exceder 3
    def removed_breakdown(group):
        g = group.sort_values('fecha').copy()
        size = len(g)
        aprobado_mask = g['resultado'].fillna('').str.lower().str.contains('aprob') | (g['nota'].notna() & (g['nota'] >= 6))
        removed_aprob = 0
        if aprobado_mask.any():
            first_ap = int(aprobado_mask.to_numpy().argmax())
            removed_aprob = max(0, size - (first_ap + 1))
            after_ap_len = min(first_ap + 1, 3)
            removed_trunc = (first_ap + 1) - after_ap_len if after_ap_len < (first_ap + 1) else 0
        else:
            removed_aprob = 0
            removed_trunc = max(0, size - 3)
        return pd.Series({'removed_aprob': removed_aprob, 'removed_trunc': removed_trunc, 'size': size})

    breakdown = completos.groupby(['id_estudiante', 'id_dictado']).apply(removed_breakdown)
    total_removed_aprob = int(breakdown['removed_aprob'].sum())
    total_removed_trunc = int(breakdown['removed_trunc'].sum())
    print(f"eliminados por existir intentos posteriores a aprobado: {total_removed_aprob}")
    print(f"eliminados por truncamiento a 3: {total_removed_trunc}")

    # Mostrar top grupos por tamaño
    print('\nTop 10 grupos por tamaño:')
    print(grp.sort_values(ascending=False).head(10))

    # Mostrar ejemplos de grupos con aprobados donde hay descartes
    print('\nEjemplos de grupos con aprobados y descartes:')
    ejemplos = []
    for (est, dic), sub in completos.groupby(['id_estudiante', 'id_dictado']):
        if len(ejemplos) >= 5:
            break
        g = sub.sort_values('fecha').copy()
        aprobado_mask = g['resultado'].fillna('').str.lower().str.contains('aprob') | (g['nota'].notna() & (g['nota'] >= 6))
        if aprobado_mask.any():
            first_ap = int(aprobado_mask.to_numpy().argmax())
            removed = len(g) - min(first_ap + 1, 3)
            if removed > 0:
                ejemplos.append({'id_estudiante': est, 'id_dictado': dic, 'size': len(g), 'first_ap_idx': first_ap, 'removed': removed})
    for e in ejemplos:
        print(e)

if __name__ == '__main__':
    main()
