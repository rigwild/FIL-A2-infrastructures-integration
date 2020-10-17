const API_PREFIX = 'http://localhost:8080'

export const api = route => fetch(`${API_PREFIX}${route}`).then(res => res.json())

export const apiAirportsList = () => api('/airports')
export const apiDateStats = (date = 'total') => api(`/dateStats/${date}`)
export const apiAirportDateStats = (aita, date = 'total') => api(`/airports/${aita}/dateStats/${date}`)
export const apiAirportDateSensorData = (aita, date, sensor) => api(`/airports/${aita}/date/${date}/sensors/${sensor}`)
