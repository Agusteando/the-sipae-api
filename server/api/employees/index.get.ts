import { pool } from '../../utils/db'
import { fetchSigniaEmployees, extractBirthdayFromCurp, resolveSigniaUrl } from '../../utils/signia'
import { normalizeAndMatchPlantel } from '../../utils/plantel'
import dayjs from 'dayjs'

export default defineEventHandler(async (event) => {
  const query = getQuery(event)
  const plantelCode = query.plantelCode as string
  const plantelNameFallback = query.plantelNameFallback as string

  console.log(`[DEBUG-HHB] Server Adapter - Requested internal code: "${plantelCode}", UI Label: "${plantelNameFallback}"`);

  let signiaData: any[] = []

  // Skip hitting Signia entirely if looking explicitly for Externals
  if (plantelCode && plantelCode !== 'EXT') {
    // Fetch all active records to prevent the 500 error caused by shortcodes
    const allEmployees = await fetchSigniaEmployees();
    
    // Process local filtering based on the resolved UI mapping
    signiaData = allEmployees.filter((emp: any) => {
      const apiPlantelName = emp.plantel?.name || emp.plantel || '';
      return normalizeAndMatchPlantel(apiPlantelName, plantelNameFallback);
    });
  }

  const [overridesRow]: any = await pool.query('SELECT * FROM overrides')
  const [externalsRow]: any = await pool.query('SELECT * FROM external_users')

  const overrideMap = new Map()
  overridesRow.forEach((o: any) => overrideMap.set(o.id, o))

  let merged = signiaData.map((emp: any) => {
    const ov = overrideMap.get(emp.id) || {}
    if (ov.baja) return null 

    const birthday = ov.birthday ? dayjs(ov.birthday).format('YYYY-MM-DD') : extractBirthdayFromCurp(emp.curp)

    return {
      ...emp,
      picture: resolveSigniaUrl(emp.picture),
      email: ov.email || emp.email,
      birthday,
      high_rank: ov.high_rank === 1,
      event_id: ov.event_id || null,
      is_external: false
    }
  }).filter(Boolean)

  // Append local external/guest users
  externalsRow.forEach((ext: any) => {
    if (ext.baja) return
    
    // Allow if EXT is explicitly requested, or if the external user's manual UI label matches the selection
    if (plantelCode === 'EXT' || ext.plantel === plantelNameFallback) {
      merged.push({
        id: ext.id,
        name: ext.name,
        plantel: { name: ext.plantel },
        email: ext.email,
        birthday: ext.birthday ? dayjs(ext.birthday).format('YYYY-MM-DD') : null,
        high_rank: ext.high_rank === 1,
        event_id: ext.event_id || null,
        picture: resolveSigniaUrl(ext.picture) || null,
        is_external: true
      })
    }
  })

  console.log(`[DEBUG-HHB] Server Adapter - Final merged records returned to client: ${merged.length}`)

  return merged.sort((a, b) => {
    if (!a.birthday) return 1
    if (!b.birthday) return -1
    return dayjs(a.birthday).format('MM-DD').localeCompare(dayjs(b.birthday).format('MM-DD'))
  })
})