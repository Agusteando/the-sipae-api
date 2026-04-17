/**
 * Capa de normalización robusta para las sedes (planteles).
 * Vincula los nombres exactos de la interfaz de usuario con los valores ERP crudos,
 * solucionando definitivamente la carga de empleados.
 */
export const plantelUItoAPI: Record<string, string[]> = {
  'ISSSTE Toluca': ['issste toluca', 'is toluca', 'ist'],
  'ISSSTE Metepec': ['issste metepec', 'is metepec', 'ism'],
  'Preescolar Metepec': ['preescolar metepec', 'pre metepec'],
  'Preescolar Toluca': ['preescolar toluca', 'pre toluca'],
  'Secundaria Toluca': ['secundaria toluca', 'sec toluca'],
  'Secundaria Metepec': ['secundaria metepec', 'sec metepec'],
  'Desarrollo Climaya': ['desarrollo climaya', 'climaya'],
  'Casita Ocoyoacac': ['casita ocoyoacac', 'ocoyoacac'],
  'Casita Metepec': ['casita metepec', 'cas metepec'],
  'Casita Toluca': ['casita toluca', 'cas toluca'],
  'Desarrollo Metepec': ['desarrollo metepec', 'des metepec'],
  'Externos e Invitados Especiales': ['externo', 'externos']
};

export function normalizeAndMatchPlantel(apiValue: string, filterLabel: string): boolean {
  if (!apiValue || !filterLabel) return false;
  
  const apiNorm = apiValue.toLowerCase().replace(/[^a-z0-9]/g, ' ');
  if (filterLabel === 'Externos e Invitados Especiales') return apiNorm.includes('externo');

  const mapping = plantelUItoAPI[filterLabel] || [];
  for (const phrase of mapping) {
    if (apiNorm.includes(phrase)) return true;
  }
  
  // Respaldo final: Verificación de coincidencia exacta
  return apiValue === filterLabel;
}