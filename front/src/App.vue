<template>
  <v-app>
    <v-app-bar app color="primary" dark>
      <div class="d-flex align-center">
        <v-img
          alt="Vuetify Logo"
          class="shrink mr-2"
          contain
          src="https://www.flaticon.com/svg/static/icons/svg/3631/3631218.svg"
          transition="scale-transition"
          width="40"
        />

        <h1>Airports sensors</h1>
      </div>
    </v-app-bar>

    <v-main>
      <v-container>
        <div v-if="isLoading" class="text-center">
          <v-progress-circular indeterminate color="primary"></v-progress-circular>
        </div>

        <div v-else>
          <h1>Global statistics</h1>
          <v-expansion-panels>
            <v-expansion-panel>
              <v-expansion-panel-header>Click to show/hide</v-expansion-panel-header>
              <v-expansion-panel-content>
                <GlobalStatistics />
              </v-expansion-panel-content>
            </v-expansion-panel>
          </v-expansion-panels>

          <h1 class="mt-5 pt-5">Airport statistics</h1>
          <v-row>
            <v-col sm="4">
              <SelectAirport @changeAirport="changeAirport" :airports="airports" />
            </v-col>
            <v-col sm="8">
              <v-card class="mx-auto" tile>
                <AirportData :airport="selectedAirport" />
              </v-card>
            </v-col>
          </v-row>
        </div>
      </v-container>
    </v-main>
  </v-app>
</template>

<script>
import { apiAirportsList } from './utils'

import GlobalStatistics from './components/GlobalStatistics'
import SelectAirport from './components/SelectAirport'
import AirportData from './components/AirportData'

export default {
  name: 'App',

  components: {
    GlobalStatistics,
    SelectAirport,
    AirportData
  },

  data() {
    return {
      isLoading: true,
      airports: null,
      selectedAirport: null
    }
  },

  async mounted() {
    this.isLoading = true
    try {
      await this.getAirportsList()
      this.selectedAirport = this.airports[0]
    } catch (error) {
    } finally {
      this.isLoading = false
    }
  },

  methods: {
    changeAirport(airport) {
      this.selectedAirport = airport
    },

    async getAirportsList() {
      const airports = await apiAirportsList()
      this.airports = airports.aitas
    }
  }
}
</script>
