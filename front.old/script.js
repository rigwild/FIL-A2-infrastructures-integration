import Vue from 'https://cdn.jsdelivr.net/npm/vue@2.6.12/dist/vue.esm.browser.js'

const API_PREFIX = 'http://localhost:8080'
const API_ROUTES = {
  airportsList: '/airports'
}

const app = new Vue({
  el: '#app',
  data() {
    return {
      message: 'Hello Vue!',
      airports: null
    }
  },
  async mounted() {
    await this.getAirportsList()
  },
  methods: {
    api(route) {
      return fetch(`${API_PREFIX}${route}`).then(res => res.json())
    },

    async getAirportsList() {
      const airports = await this.api(API_ROUTES.airportsList)
      this.airports = airports.aitas
    }
  }
})
