<template>
  <v-container>
    <h1>{{ airport.Aita }} - {{ airport.Name }}</h1>

    <!-- Airport statistics -->
    <h2 class="py-2">Airport all-time statistics</h2>
    <StatsLoader :promise="loadAirportStats()" />

    <v-divider class="my-3"></v-divider>

    <!-- Date selector -->
    <h2 class="py-2">Airport date statistics</h2>
    <v-menu
      v-model="dateMenu"
      :close-on-content-click="false"
      :nudge-right="40"
      transition="scale-transition"
      offset-y
      min-width="290px"
    >
      <template v-slot:activator="{ on, attrs }">
        <v-text-field
          v-model="date"
          label="Select a date"
          prepend-icon="mdi-calendar"
          readonly
          v-bind="attrs"
          v-on="on"
        ></v-text-field>
      </template>
      <v-date-picker v-model="date" @input="dateMenu = false"></v-date-picker>
    </v-menu>

    <!-- Date-specific statistics -->
    <h3 class="py-2">Airport {{ date }} day statistics</h3>
    <StatsLoader :promise="loadAirportStats(date)" />

    <!-- Month statistics -->
    <h3 class="py-2">Airport {{ dateMonth }} month statistics</h3>
    <StatsLoader :promise="loadAirportStats(dateMonth)" />

    <!-- Year statistics -->
    <h3 class="py-2">Airport {{ dateYear }} year statistics</h3>
    <StatsLoader :promise="loadAirportStats(dateYear)" />

    <!-- Temperature graph -->
    <h2 class="py-2">Airport temperature sensor data</h2>
    <SensorDataLoader :promise="loadAirportDateSensorData(date, 'temperature')" type="temperature" />
    <!-- Wind graph -->
    <h2 class="py-2">Airport wind sensor data</h2>
    <SensorDataLoader :promise="loadAirportDateSensorData(date, 'wind')" type="wind" />
    <!-- Pressure graph -->
    <h2 class="py-2">Airport pressure sensor data</h2>
    <SensorDataLoader :promise="loadAirportDateSensorData(date, 'pressure')" type="pressure" />
  </v-container>
</template>

<script>
import { apiAirportDateStats, apiAirportDateSensorData } from '../utils'

import StatsLoader from './StatsLoader'
import SensorDataLoader from './SensorDataLoader'

export default {
  components: {
    StatsLoader,
    SensorDataLoader
  },
  props: {
    airport: {
      required: true,
      type: Object
    }
  },
  computed: {
    dateMonth() {
      return this.date.slice(0, 7)
    },
    dateYear() {
      return this.date.slice(0, 4)
    }
  },
  data() {
    return {
      dateMenu: null,
      date: new Date().toISOString().substr(0, 10)
    }
  },
  methods: {
    loadAirportStats(_date = 'total') {
      return apiAirportDateStats(this.airport.Aita, _date)
    },
    loadAirportDateSensorData(_date, sensor) {
      return apiAirportDateSensorData(this.airport.Aita, _date, sensor)
    }
  }
}
</script>
