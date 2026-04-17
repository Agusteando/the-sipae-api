/**
 * Fully restored fetch pipeline to allow shared usage across the server.
 * Removed the destructive 'plantelId' parameter to prevent 500 Internal Server Errors.
 * Data is now fetched globally and filtered safely on our own local adapter.
 */
export const fetchSigniaEmployees = async () => {
  const url = 'https://signia.casitaapps.com/api/export/employees?isActive=true';

  console.log(`[DEBUG-HHB] Signia API - Fetching URL: ${url}`);
  try {
    const response = await fetch(url);
    console.log(`[DEBUG-HHB] Signia API - Response Status: ${response.status}`);
    
    if (!response.ok) {
      console.error(`[DEBUG-HHB] Signia API - Error: ${response.statusText}`);
      return [];
    }
    
    const data = await response.json();
    const arrayData = Array.isArray(data) ? data : [];
    console.log(`[DEBUG-HHB] Signia API - Returned ${arrayData.length} active records globally.`);
    return arrayData;
  } catch (e) {
    console.error('[DEBUG-HHB] Signia API - Request failed:', e);
    return [];
  }
}

export const extractBirthdayFromCurp = (curp?: string): string | null => {
  if (!curp || curp.length < 18) return null
  const yy = parseInt(curp.substring(4, 6))
  const mm = curp.substring(6, 8)
  const dd = curp.substring(8, 10)
  const centuryChar = curp.charAt(16)
  
  let year = 1900 + yy
  if (/[A-Z]/.test(centuryChar)) {
    year = 2000 + yy
  } else if (/[0-9]/.test(centuryChar)) {
    year = 1900 + yy
    if (yy < 25 && year < 1925) year = 2000 + yy
  }
  return `${year}-${mm}-${dd}`
}

export const resolveSigniaUrl = (url?: string): string | null => {
  if (!url) return null;
  
  let resolved = url;
  
  if (url.startsWith('/storage') || url.startsWith('storage/')) {
    const path = url.startsWith('/') ? url : `/${url}`;
    resolved = `https://signia.casitaapps.com${path}`;
  } else if (url.startsWith('http://localhost') || url.startsWith('https://localhost')) {
    resolved = url.replace(/^https?:\/\/localhost(:\d+)?/, 'https://signia.casitaapps.com');
  }
  
  return resolved;
}