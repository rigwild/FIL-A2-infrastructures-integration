<template>
  <v-container>
    <!-- All-time statistics -->
    <h2 class="py-2">All-time statistics</h2>
    <StatsLoader :promise="loadGlobalStats()" />

    <v-divider class="my-3"></v-divider>

    <!-- Date selector -->
    <h2 class="py-2">Date statistics</h2>
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
    <h3 class="py-2">{{ date }} day statistics</h3>
    <StatsLoader :promise="loadGlobalStats(date)" />

    <!-- Month statistics -->
    <h3 class="py-2">{{ dateMonth }} month statistics</h3>
    <StatsLoader :promise="loadGlobalStats(dateMonth)" />

    <!-- Year statistics -->
    <h3 class="py-2">{{ dateYear }} year statistics</h3>
    <StatsLoader :promise="loadGlobalStats(dateYear)" />
  </v-container>
</template>

<script>
import { apiDateStats } from '../utils'

import StatsLoader from './StatsLoader'

export default {
  components: {
    StatsLoader
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
    loadGlobalStats(date = 'total') {
      return apiDateStats(date)
    }
  }
}
</script>
