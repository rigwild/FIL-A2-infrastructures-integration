<template>
  <v-card class="mx-auto" tile>
    <v-list rounded class="overflow-y-auto" max-height="400">
      <v-subheader>Airports</v-subheader>
      <v-list-item-group v-model="selected" color="primary">
        <v-list-item v-for="(airport, i) in airports" :key="i" @click="changeAirport(airport)">
          <v-list-item-icon>
            <v-icon>mdi-airplane</v-icon>
          </v-list-item-icon>
          <v-list-item-content>
            <v-list-item-title>{{ airport.Name }}</v-list-item-title>
          </v-list-item-content>
          <v-btn fab small depressed color="primary">{{ airport.Aita }}</v-btn>
        </v-list-item>
      </v-list-item-group>
    </v-list>
  </v-card>
</template>

<script>
export default {
  props: {
    airports: {
      required: true,
      type: Array
    }
  },
  data() {
    return {
      selected: 0
    }
  },
  methods: {
    changeAirport(airport) {
      this.$emit('changeAirport', airport)
    }
  }
}
</script>
